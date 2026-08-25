#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>
#include <algorithm>
#include <stdexcept>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int FILE_DOESNT_EXIST;
}
}

namespace DB::Cas
{

namespace
{

/// The windowed slice of `data` for `range`, with the same clamping `get` documents: an offset at or
/// past EOF yields an empty result; an open-ended length runs to EOF. Shared by `get` and `getStream`
/// so the two stay in lockstep.
String sliceWindow(const String & data, Range range)
{
    const size_t offset = static_cast<size_t>(range.offset);
    if (offset >= data.size())
        return {};
    if (range.length.has_value())
        return data.substr(offset, static_cast<size_t>(*range.length));
    return data.substr(offset);
}

}

Token InMemoryBackend::mintToken()
{
    Token t;
    t.value = std::to_string(++token_seq_);
    t.type = TokenType::Emulated;
    return t;
}

std::optional<GetResult> InMemoryBackend::get(const String & key, Range range)
{
    std::lock_guard lock(mutex_);
    auto it = store_.find(key);
    if (it == store_.end())
        return std::nullopt;

    GetResult gr;
    gr.bytes = sliceWindow(it->second.bytes, range);
    gr.token = it->second.token;
    gr.attributes = it->second.meta;
    return gr;
}

std::optional<GetStreamResult> InMemoryBackend::getStream(const String & key, Range range)
{
    std::lock_guard lock(mutex_);
    auto it = store_.find(key);
    if (it == store_.end())
        return std::nullopt;

    /// Copy the windowed bytes into an owning buffer — the in-memory backend has no separate storage
    /// to stream from, so the "stream" reads from a private copy of exactly the requested window.
    GetStreamResult sr;
    sr.stream = std::make_unique<ReadBufferFromOwnString>(sliceWindow(it->second.bytes, range));
    sr.token = it->second.token;
    return sr;
}

HeadResult InMemoryBackend::head(const String & key)
{
    std::lock_guard lock(mutex_);
    auto it = store_.find(key);
    if (it == store_.end())
        return HeadResult{};

    HeadResult hr;
    hr.exists = true;
    hr.size = static_cast<uint64_t>(it->second.bytes.size());
    hr.token = it->second.token;
    hr.attributes = it->second.meta;
    return hr;
}

PutResult InMemoryBackend::putIfAbsent(const String & key, const String & bytes, const ObjectMeta & meta)
{
    std::lock_guard lock(mutex_);

    // One-shot injected ambiguous outcome: throw WITHOUT touching the store, modeling a request whose
    // own attempt outcome never reached the caller (see the header doc for the classification this
    // must produce). std::runtime_error, not DB::Exception, is deliberate: it dodges BOTH
    // classification paths in BOTH build configurations -- dynamic_cast<const Exception *> fails (so
    // isDeterministicLocalFailure is never consulted), and classifyConditionalWriteResult falls through
    // to its Unresolved default because it isn't an S3Exception. A DB::Exception would have been
    // fragile: picking a code outside isDeterministicLocalFailure's set is a landmine for the next
    // person who extends that set.
    auto ambiguous_it = ambiguous_put_keys_.find(key);
    if (ambiguous_it != ambiguous_put_keys_.end())
    {
        ambiguous_put_keys_.erase(ambiguous_it);
        throw std::runtime_error("InMemoryBackend: injected ambiguous putIfAbsent outcome for '" + key + "'");
    }

    if (store_.contains(key))
        return {PutOutcome::PreconditionFailed, {}};

    Token t = mintToken();
    Object obj;
    obj.bytes = bytes;
    obj.token = t;
    obj.meta = meta;
    store_[key] = std::move(obj);
    return {PutOutcome::Done, t};
}

void InMemoryBackend::publishBlob(const BlobPublishRequest & request)
{
    if (const auto * streaming = std::get_if<StreamingBlobPublication>(&request.publication))
    {
        std::unique_ptr<ReadBuffer> payload = streaming->open_payload();
        if (!payload)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "InMemoryBackend::publishBlob: payload source for {} returned no reader",
                request.destination_key);

        /// Drain before taking the store lock: the source may itself read another object from this
        /// backend. The complete body remains private until its size has been validated.
        String body = streaming->fresh_envelope;
        blob_publication_detail::BlobPayloadCopyResult copy_result;
        {
            WriteBufferFromString out(body, AppendModeTag{});
            copy_result = blob_publication_detail::copyBlobPayloadBounded(*payload, out, streaming->payload_size);
            if (copy_result.exact(streaming->payload_size))
                out.finalize();
            else
                out.cancel();
        }

        if (!copy_result.exact(streaming->payload_size))
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "InMemoryBackend::publishBlob: source yielded {}{} payload bytes for {}, declared {} -- nothing was published",
                copy_result.has_excess ? "more than " : "",
                copy_result.copied,
                request.destination_key,
                streaming->payload_size);

        std::lock_guard lock(mutex_);
        Object object;
        object.bytes = std::move(body);
        object.token = mintToken();
        store_[request.destination_key] = std::move(object);
        return;
    }

    const auto & staged = std::get<VerbatimStagedBlobPublication>(request.publication);
    std::lock_guard lock(mutex_);
    const auto source = store_.find(staged.object_key);
    if (source == store_.end())
        throw Exception(
            ErrorCodes::FILE_DOESNT_EXIST,
            "InMemoryBackend::publishBlob: staging object {} is absent",
            staged.object_key);

    Object object;
    object.bytes = source->second.bytes;
    object.token = mintToken();
    store_[request.destination_key] = std::move(object);
}

PutResult InMemoryBackend::putOverwrite(const String & key, const String & bytes, const Token & expected, const ObjectMeta & meta)
{
    std::lock_guard lock(mutex_);
    auto it = store_.find(key);
    if (it == store_.end())
        return {PutOutcome::PreconditionFailed, {}};

    if (enforce_tokens_ && it->second.token != expected)
        return {PutOutcome::PreconditionFailed, {}};

    Token t = mintToken();
    it->second.bytes = bytes;
    it->second.token = t;
    it->second.meta = meta;
    return {PutOutcome::Done, t};
}

CasResult InMemoryBackend::casPut(const String & key, const String & bytes, const std::optional<Token> & expected, const ObjectMeta & meta)
{
    std::lock_guard lock(mutex_);

    // One-shot injected conflict
    auto fail_it = fail_next_cas_.find(key);
    if (fail_it != fail_next_cas_.end())
    {
        fail_next_cas_.erase(fail_it);
        return {CasOutcome::Conflict, {}};
    }

    auto it = store_.find(key);
    bool exists = (it != store_.end());

    if (!expected.has_value())
    {
        // create-if-absent CAS
        if (exists)
            return {CasOutcome::Conflict, {}};
        Token t = mintToken();
        Object obj;
        obj.bytes = bytes;
        obj.token = t;
        obj.meta = meta;
        store_[key] = std::move(obj);
        return {CasOutcome::Committed, t};
    }
    else
    {
        // swap-if-current CAS
        if (!exists)
            return {CasOutcome::Conflict, {}};
        if (enforce_tokens_ && it->second.token != *expected)
            return {CasOutcome::Conflict, {}};
        Token t = mintToken();
        it->second.bytes = bytes;
        it->second.token = t;
        it->second.meta = meta;
        return {CasOutcome::Committed, t};
    }
}

DeleteOutcome InMemoryBackend::applyDelete(const String & key, const Token & token)
{
    // Caller holds the mutex.
    auto it = store_.find(key);
    if (it == store_.end())
    {
        DeleteOutcome d;
        d.kind = DeleteOutcome::Kind::NotFound;
        return d;
    }

    if (enforce_tokens_ && it->second.token != token)
    {
        DeleteOutcome d;
        d.kind = DeleteOutcome::Kind::TokenMismatch;
        return d;
    }

    store_.erase(it);
    DeleteOutcome d;
    d.kind = DeleteOutcome::Kind::Deleted;
    d.created_delete_marker = simulate_delete_markers_;
    return d;
}

DeleteOutcome InMemoryBackend::deleteExact(const String & key, const Token & token)
{
    std::lock_guard lock(mutex_);

    if (hold_deletes_)
    {
        // Validate the key exists (and token matches if enforcing) before queuing,
        // but don't remove yet — just enqueue.
        auto it = store_.find(key);
        if (it == store_.end())
        {
            DeleteOutcome d;
            d.kind = DeleteOutcome::Kind::NotFound;
            return d;
        }
        if (enforce_tokens_ && it->second.token != token)
        {
            DeleteOutcome d;
            d.kind = DeleteOutcome::Kind::TokenMismatch;
            return d;
        }
        PendingDelete pd;
        pd.key = key;
        pd.token = token;
        pending_deletes_.push_back(std::move(pd));
        DeleteOutcome d;
        d.kind = DeleteOutcome::Kind::Deleted;
        d.created_delete_marker = simulate_delete_markers_;
        return d;
    }

    return applyDelete(key, token);
}

ListPage InMemoryBackend::list(const String & prefix, const String & cursor, size_t limit)
{
    if (limit == 0)
        return {};

    std::lock_guard lock(mutex_);
    ListPage page;

    // Cursor is the last key returned by the previous page.
    auto it = cursor.empty() ? store_.lower_bound(prefix) : store_.upper_bound(cursor);

    size_t count = 0;
    while (it != store_.end() && count < limit)
    {
        if (!it->first.starts_with(prefix))
            break;

        ListedKey lk;
        lk.key = it->first;
        lk.size = static_cast<uint64_t>(it->second.bytes.size());
        lk.token = it->second.token;   /// in-memory backend always surfaces the token (supportsListTokens == true)
        page.keys.push_back(std::move(lk));
        ++count;
        ++it;
    }

    // Set next_cursor if there are more keys in this prefix
    if (!page.keys.empty() && it != store_.end() && it->first.starts_with(prefix))
        page.next_cursor = page.keys.back().key;

    return page;
}

void InMemoryBackend::setHoldDeletes(bool hold)
{
    std::lock_guard lock(mutex_);
    hold_deletes_ = hold;
}

size_t InMemoryBackend::pendingDeletes() const
{
    std::lock_guard lock(mutex_);
    return pending_deletes_.size();
}

DeleteOutcome InMemoryBackend::landPendingDelete(size_t i)
{
    std::lock_guard lock(mutex_);
    if (i >= pending_deletes_.size())
    {
        DeleteOutcome d;
        d.kind = DeleteOutcome::Kind::NotFound;
        return d;
    }

    PendingDelete pd = pending_deletes_[i];
    pending_deletes_.erase(pending_deletes_.begin() + static_cast<ptrdiff_t>(i));

    // Apply the token check at LAND time — the object may have been modified since the delete was enqueued.
    return applyDelete(pd.key, pd.token);
}

void InMemoryBackend::failNextCasPut(const String & key)
{
    std::lock_guard lock(mutex_);
    fail_next_cas_.insert(key);
}

void InMemoryBackend::injectAmbiguousPutIfAbsent(const String & key)
{
    std::lock_guard lock(mutex_);
    ambiguous_put_keys_.insert(key);
}

void InMemoryBackend::setEnforceTokens(bool enforce)
{
    std::lock_guard lock(mutex_);
    enforce_tokens_ = enforce;
}

void InMemoryBackend::setSimulateDeleteMarkers(bool simulate)
{
    std::lock_guard lock(mutex_);
    simulate_delete_markers_ = simulate;
}

}
