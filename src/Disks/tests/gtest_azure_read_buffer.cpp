#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <algorithm>
#include <filesystem>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <Backups/BackupIO_AzureBlobStorage.h>
#include <Common/tests/gtest_global_context.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <Disks/DiskObjectStorage/ObjectStorages/AzureBlobStorage/AzureObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ReadBufferFromAzureBlobStorage.h>
#include <IO/AzureBlobStorage/copyAzureBlobStorageFile.h>
#include <IO/ReadHelpers.h>
#include <IO/SharedThreadPools.h>
#include <Interpreters/Context.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheFactory.h>
#include <Interpreters/FileCache/FileCacheSettings.h>
#include <Core/ServerUUID.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueuePostProcessor.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueSource.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueTableMetadata.h>

#include <azure/core/http/raw_response.hpp>
#include <azure/core/http/transport.hpp>
#include <azure/core/io/body_stream.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>

#include <base/scope_guard.h>

#include <gtest/gtest.h>

namespace DB::FileCacheSetting
{
    extern const FileCacheSettingsString path;
    extern const FileCacheSettingsUInt64 max_size;
    extern const FileCacheSettingsUInt64 max_elements;
    extern const FileCacheSettingsUInt64 max_file_segment_size;
    extern const FileCacheSettingsUInt64 boundary_alignment;
    extern const FileCacheSettingsUInt64 background_download_threads;
}

namespace DB::ErrorCodes
{
    extern const int UNEXPECTED_END_OF_FILE;
    extern const int HTTP_RANGE_NOT_SATISFIABLE;
    extern const int FILE_CHANGED_DURING_READ;
    extern const int BACKUP_DAMAGED;
    extern const int AZURE_BLOB_STORAGE_ERROR;
    extern const int FILE_DOESNT_EXIST;
}

namespace
{

/// A body stream that reports a `Content-Length` unrelated to the amount of data it holds,
/// the way a malicious or broken remote endpoint can.
class LyingBodyStream : public Azure::Core::IO::BodyStream
{
public:
    LyingBodyStream(std::vector<uint8_t> data_, int64_t reported_length_)
        : data(std::move(data_)), reported_length(reported_length_)
    {
    }

    int64_t Length() const override { return reported_length; }

    void Rewind() override { position = 0; }

private:
    size_t OnRead(uint8_t * buffer, size_t count, const Azure::Core::Context &) override
    {
        const size_t to_read = std::min(count, data.size() - position);
        /// `memcpy` must not be called with a null source, and an empty response has one.
        if (to_read != 0)
            memcpy(buffer, data.data() + position, to_read);
        position += to_read;
        return to_read;
    }

    std::vector<uint8_t> data;
    int64_t reported_length;
    size_t position = 0;
};

/// The bytes of the blob at positions `start`..`start + size - 1`: every byte of the blob holds
/// the low 8 bits of its position, so that every byte can be attributed to its position.
std::vector<uint8_t> countingBytes(size_t start, size_t size)
{
    std::vector<uint8_t> data(size);
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<uint8_t>(start + i);
    return data;
}

void assertCountsUpFromZero(const std::string & data)
{
    for (size_t i = 0; i < data.size(); ++i)
        ASSERT_EQ(static_cast<uint8_t>(data[i]), static_cast<uint8_t>(i)) << "at position " << i;
}

/// How the endpoint handles the generation of the object across the several requests of one
/// logical read: which `ETag` it reports, whether that tag changes under the reader, and whether
/// it evaluates the `If-Match` precondition the way a real endpoint does.
struct ETagBehaviour
{
    static constexpr const char * first_generation = "\"0x8DA000000000000\"";
    static constexpr const char * second_generation = "\"0x8DA111111111111\"";
    /// The same tag as `first_generation` in the spelling of a blob listing, whose `Etag` element
    /// carries the bare tag, while the `ETag` header of a response carries it quoted.
    static constexpr const char * first_generation_bare = "0x8DA000000000000";

    /// The `ETag` reported by the first response.
    std::string etag = first_generation;
    /// The `ETag` reported from the second response on. Empty means the object never changes.
    std::string etag_after_first;
    /// A real endpoint answers `412 Precondition Failed` when `If-Match` does not hold; one that
    /// ignores the precondition serves the new generation instead.
    bool honour_if_match = false;
};

/// Serves a blob whose every byte holds the low 8 bits of its position, but misbehaves in the
/// two ways a remote endpoint can: every response carries at most `max_response_size` bytes no
/// matter how much was requested (an endpoint that caps or truncates its ranged responses), and
/// no byte at position `served_size` or beyond is ever served (a blob that is shorter than the
/// caller believes), while `blob_size` is what the endpoint claims in `Content-Range`. With
/// `ignore_range`, the endpoint disregards the requested range altogether and answers every
/// request with `200 OK` and the object from byte 0, the way an endpoint that does not support
/// ranged requests does. With `blob_size_after_first`, the total advertised in `Content-Range`
/// changes to that value from the second response on (an endpoint whose idea of the object size
/// is not stable across the requests of one read). With `refuse_range_past_the_data`, a ranged
/// request that begins at `served_size` or beyond is answered with `416 Range Not Satisfiable`, the
/// way a real endpoint answers a range past the end of a blob, instead of with an empty body; with
/// `blob_missing`, a `HEAD` is answered with `404 Not Found`, the way one for a blob that is not
/// there is.
class MisbehavingRangeTransport : public Azure::Core::Http::HttpTransport
{
public:
    MisbehavingRangeTransport(
        size_t max_response_size_,
        size_t served_size_,
        size_t blob_size_,
        bool send_etag_,
        std::optional<int64_t> reported_length_ = {},
        bool ignore_range_ = false,
        ETagBehaviour etags_ = {},
        std::optional<size_t> blob_size_after_first_ = {},
        bool refuse_range_past_the_data_ = false,
        bool blob_missing_ = false)
        : max_response_size(max_response_size_)
        , served_size(served_size_)
        , blob_size(blob_size_)
        , send_etag(send_etag_)
        , reported_length(reported_length_)
        , ignore_range(ignore_range_)
        , etags(std::move(etags_))
        , blob_size_after_first(blob_size_after_first_)
        , refuse_range_past_the_data(refuse_range_past_the_data_)
        , blob_missing(blob_missing_)
    {
    }

    std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request & request, const Azure::Core::Context & context) override
    {
        const bool first_response = responses_sent == 0;
        const std::string & current_etag = overwritten_etag
            ? *overwritten_etag
            : (first_response || etags.etag_after_first.empty()) ? etags.etag : etags.etag_after_first;
        const size_t current_blob_size = first_response ? blob_size : blob_size_after_first.value_or(blob_size);
        ++responses_sent;

        /// The endpoint also accepts uploads (`Put Blob`, `Put Block` and `Put Block List`), so a
        /// read-and-write copy can be driven end to end through it. The bytes of every uploaded
        /// blob or block are appended to `uploaded` in the order they arrive; the block list itself
        /// is XML and is not part of the data.
        /// A native blob-to-blob copy (`Copy Blob From URL` and `Copy Blob`, both a `PUT` with
        /// `x-ms-copy-source`) transfers nothing through the client, so what it copies is whatever
        /// generation of the source the endpoint holds at that moment; the generation is recorded so a
        /// test can tell which one was copied. A real endpoint evaluates `x-ms-source-if-match` against
        /// the source and answers `412 Precondition Failed` when it does not hold.
        if (request.GetMethod() == Azure::Core::Http::HttpMethod::Put && request.GetHeader("x-ms-copy-source").HasValue())
        {
            if (auto source_if_match = request.GetHeader("x-ms-source-if-match"); source_if_match.HasValue())
            {
                source_if_match_headers.push_back(source_if_match.Value());
                if (etags.honour_if_match && source_if_match.Value() != current_etag)
                    return preconditionFailed();
            }
            else
                source_if_match_headers.emplace_back();

            natively_copied_generations.push_back(current_etag);

            auto accepted = std::make_unique<Azure::Core::Http::RawResponse>(1, 1, Azure::Core::Http::HttpStatusCode::Accepted, "Accepted");
            accepted->SetHeader("Content-Length", "0");
            accepted->SetHeader("ETag", current_etag);
            accepted->SetHeader("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
            accepted->SetHeader("x-ms-copy-id", "copy-id");
            accepted->SetHeader("x-ms-copy-status", "success");
            accepted->SetBodyStream(std::make_unique<LyingBodyStream>(std::vector<uint8_t>{}, 0));
            return accepted;
        }

        /// `Delete Blob`. A real endpoint evaluates `If-Match` against the current generation and
        /// answers `412 Precondition Failed` when it does not hold; the generation deleted (if any)
        /// and the header of every request are recorded so a test can tell what was deleted.
        if (request.GetMethod() == Azure::Core::Http::HttpMethod::Delete)
        {
            if (auto if_match = request.GetHeader("if-match"); if_match.HasValue())
            {
                delete_if_match_headers.push_back(if_match.Value());
                if (etags.honour_if_match && if_match.Value() != current_etag)
                    return preconditionFailed();
            }
            else
                delete_if_match_headers.emplace_back();

            deleted_generations.push_back(current_etag);

            auto accepted = std::make_unique<Azure::Core::Http::RawResponse>(1, 1, Azure::Core::Http::HttpStatusCode::Accepted, "Accepted");
            accepted->SetHeader("Content-Length", "0");
            accepted->SetBodyStream(std::make_unique<LyingBodyStream>(std::vector<uint8_t>{}, 0));
            return accepted;
        }

        /// The properties of the destination, polled by an asynchronous copy until it completes.
        /// The headers below are the ones the SDK reads from every such response.
        if (request.GetMethod() == Azure::Core::Http::HttpMethod::Head)
        {
            ++head_requests;
            if (blob_missing)
                return notFound();
            auto properties = std::make_unique<Azure::Core::Http::RawResponse>(1, 1, Azure::Core::Http::HttpStatusCode::Ok, "OK");
            properties->SetHeader("Content-Length", std::to_string(blob_size));
            if (send_etag)
                properties->SetHeader("ETag", current_etag);
            properties->SetHeader("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
            properties->SetHeader("x-ms-creation-time", "Wed, 21 Oct 2015 07:28:00 GMT");
            properties->SetHeader("x-ms-blob-type", "BlockBlob");
            properties->SetHeader("x-ms-lease-state", "available");
            properties->SetHeader("x-ms-lease-status", "unlocked");
            properties->SetHeader("x-ms-server-encrypted", "true");
            properties->SetHeader("x-ms-copy-id", "copy-id");
            properties->SetHeader("x-ms-copy-status", "success");
            properties->SetHeader("x-ms-copy-progress", std::to_string(blob_size) + "/" + std::to_string(blob_size));
            properties->SetHeader("x-ms-copy-source", "http://azure.invalid/container/blob");
            properties->SetHeader("x-ms-copy-completion-time", "Wed, 21 Oct 2015 07:28:00 GMT");
            properties->SetBodyStream(std::make_unique<LyingBodyStream>(std::vector<uint8_t>{}, 0));
            return properties;
        }

        if (request.GetMethod() == Azure::Core::Http::HttpMethod::Put)
        {
            const auto & query = request.GetUrl().GetQueryParameters();
            const auto comp = query.find("comp");
            if (comp == query.end() || comp->second != "blocklist")
            {
                if (auto * body = request.GetBodyStream())
                {
                    auto bytes = body->ReadToEnd(context);
                    uploaded.append(reinterpret_cast<const char *>(bytes.data()), bytes.size());
                }
            }

            auto created = std::make_unique<Azure::Core::Http::RawResponse>(1, 1, Azure::Core::Http::HttpStatusCode::Created, "Created");
            created->SetHeader("Content-Length", "0");
            created->SetHeader("ETag", current_etag);
            created->SetHeader("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
            /// The SDK reads this header unconditionally from every upload response.
            created->SetHeader("x-ms-request-server-encrypted", "false");
            created->SetBodyStream(std::make_unique<LyingBodyStream>(std::vector<uint8_t>{}, 0));
            return created;
        }

        if (etags.honour_if_match)
        {
            if (auto if_match = request.GetHeader("if-match"); if_match.HasValue() && if_match.Value() != current_etag)
                return preconditionFailed();
        }

        /// "x-ms-range: bytes=<start>-<end>", where "-<end>" is optional.
        size_t range_start = 0;
        if (auto range = request.GetHeader("x-ms-range"); range.HasValue() && !ignore_range)
        {
            const std::string & value = range.Value();
            if (const size_t eq_pos = value.find('='); eq_pos != std::string::npos)
                range_start = std::stoull(value.substr(eq_pos + 1));
        }

        const size_t response_size = range_start < served_size
            ? std::min(max_response_size, served_size - range_start)
            : 0;

        /// A real endpoint refuses a range that begins where it has nothing to serve, rather than
        /// answering with an empty body. That refusal is the only positive statement about the end
        /// of an object whose size the reader does not know locally. A blob that has no bytes at
        /// all is a case of its own: there is no satisfiable range of it to report a requested one
        /// against, so Azure and Azurite answer `400 Bad Request` with `InvalidRange` there and
        /// `416 Range Not Satisfiable` everywhere else.
        if (refuse_range_past_the_data && !ignore_range && range_start >= served_size)
            return served_size == 0 ? invalidRange() : rangeNotSatisfiable();

        const size_t range_end = range_start + (response_size == 0 ? 0 : response_size - 1);

        auto response = ignore_range
            ? std::make_unique<Azure::Core::Http::RawResponse>(1, 1, Azure::Core::Http::HttpStatusCode::Ok, "OK")
            : std::make_unique<Azure::Core::Http::RawResponse>(1, 1, Azure::Core::Http::HttpStatusCode::PartialContent, "Partial Content");

        /// The length of the body as the endpoint chooses to report it, which is not necessarily
        /// the number of bytes the body actually holds.
        const int64_t length_to_report = reported_length.value_or(static_cast<int64_t>(response_size));

        response->SetHeader("Content-Length", std::to_string(length_to_report));
        /// A `200 OK` response to a ranged request carries no `Content-Range`.
        if (!ignore_range)
            response->SetHeader(
                "Content-Range",
                "bytes " + std::to_string(range_start) + "-" + std::to_string(range_end) + "/" + std::to_string(current_blob_size));
        response->SetHeader("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT");
        if (send_etag)
            response->SetHeader("ETag", current_etag);
        response->SetBodyStream(std::make_unique<LyingBodyStream>(countingBytes(range_start, response_size), length_to_report));

        return response;
    }

    /// Everything the endpoint has been asked to store, in upload order.
    const std::string & uploadedData() const { return uploaded; }

    /// The generation of the source (its `ETag` at that moment) that each native copy transferred.
    const std::vector<std::string> & nativelyCopiedGenerations() const { return natively_copied_generations; }

    /// The `x-ms-source-if-match` header of each native copy request, empty when it carried none.
    const std::vector<std::string> & sourceIfMatchHeaders() const { return source_if_match_headers; }

    /// The generation (its `ETag` at that moment) that each successful `Delete Blob` removed.
    const std::vector<std::string> & deletedGenerations() const { return deleted_generations; }

    /// The `If-Match` header of each `Delete Blob` request, empty when it carried none.
    const std::vector<std::string> & deleteIfMatchHeaders() const { return delete_if_match_headers; }

    /// How many times the properties of the object were asked for with a `HEAD`.
    size_t headRequests() const { return head_requests; }

    /// The object is overwritten by somebody else: from now on the endpoint holds the generation
    /// `new_etag`, whatever `ETagBehaviour` said. Lets a test place the overwrite at an exact point
    /// of a sequence of requests, such as between the copy and the delete of a move.
    void overwriteObject(const std::string & new_etag) { overwritten_etag = new_etag; }

private:
    static std::unique_ptr<Azure::Core::Http::RawResponse> preconditionFailed()
    {
        auto failure = std::make_unique<Azure::Core::Http::RawResponse>(
            1, 1, Azure::Core::Http::HttpStatusCode::PreconditionFailed, "The condition specified using HTTP conditional header(s) is not met.");
        failure->SetHeader("Content-Length", "0");
        failure->SetBodyStream(std::make_unique<LyingBodyStream>(std::vector<uint8_t>{}, 0));
        return failure;
    }

    static std::unique_ptr<Azure::Core::Http::RawResponse> rangeNotSatisfiable()
    {
        auto failure = std::make_unique<Azure::Core::Http::RawResponse>(
            1, 1, Azure::Core::Http::HttpStatusCode::RangeNotSatisfiable, "The range specified is invalid for the current size of the resource.");
        failure->SetHeader("Content-Length", "0");
        failure->SetBodyStream(std::make_unique<LyingBodyStream>(std::vector<uint8_t>{}, 0));
        return failure;
    }

    /// How Azure answers a ranged `GET` of a blob that has no bytes: the status is `400`, and the
    /// reason is in the `x-ms-error-code` header the SDK reads into `RequestFailedException::ErrorCode`.
    static std::unique_ptr<Azure::Core::Http::RawResponse> invalidRange()
    {
        auto failure = std::make_unique<Azure::Core::Http::RawResponse>(
            1, 1, Azure::Core::Http::HttpStatusCode::BadRequest, "The range specified is invalid for the current size of the resource.");
        failure->SetHeader("Content-Length", "0");
        failure->SetHeader("x-ms-error-code", "InvalidRange");
        failure->SetBodyStream(std::make_unique<LyingBodyStream>(std::vector<uint8_t>{}, 0));
        return failure;
    }

    static std::unique_ptr<Azure::Core::Http::RawResponse> notFound()
    {
        auto failure = std::make_unique<Azure::Core::Http::RawResponse>(
            1, 1, Azure::Core::Http::HttpStatusCode::NotFound, "The specified blob does not exist.");
        failure->SetHeader("Content-Length", "0");
        failure->SetBodyStream(std::make_unique<LyingBodyStream>(std::vector<uint8_t>{}, 0));
        return failure;
    }

    size_t max_response_size;
    size_t served_size;
    size_t blob_size;
    bool send_etag;
    std::optional<int64_t> reported_length;
    bool ignore_range;
    ETagBehaviour etags;
    std::optional<size_t> blob_size_after_first;
    bool refuse_range_past_the_data;
    bool blob_missing;
    size_t responses_sent = 0;
    std::string uploaded;
    std::vector<std::string> natively_copied_generations;
    std::vector<std::string> source_if_match_headers;
    std::vector<std::string> deleted_generations;
    std::vector<std::string> delete_if_match_headers;
    std::optional<std::string> overwritten_etag;
    size_t head_requests = 0;
};

/// Reads a blob from an endpoint that answers every ranged request with at most
/// `max_response_size` bytes and never serves a byte at position `served_size` or beyond,
/// with the right bound set to `read_until_position` and a `buffer_size`-byte reading buffer.
std::string readWithRightBound(
    size_t max_response_size,
    size_t served_size,
    size_t blob_size,
    size_t read_until_position,
    size_t buffer_size,
    size_t max_read_retries = 1,
    bool send_etag = true,
    bool ignore_range = false)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = std::make_shared<MisbehavingRangeTransport>(
        max_response_size, served_size, blob_size, send_etag, /* reported_length */ std::nullopt, ignore_range);

    auto container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options), /* blob_prefix */ "");

    DB::ReadSettings read_settings;
    read_settings.remote_fs_settings.buffer_size = buffer_size;

    DB::ReadBufferFromAzureBlobStorage buffer(
        container_client,
        "blob",
        read_settings,
        max_read_retries,
        /* max_single_download_retries */ 1);

    buffer.setReadUntilPosition(read_until_position);

    std::string result;
    DB::readStringUntilEOF(result, buffer);
    return result;
}

/// Reads a whole blob of `blob_size` bytes without any right bound, from an endpoint that answers
/// every request with at most `max_response_size` bytes and reports `reported_length` as the
/// `Content-Length` of every response no matter how many bytes it actually sends. With
/// `blob_size_after_first`, every response after the first advertises that total in `Content-Range`.
std::string readWithoutRightBound(
    size_t max_response_size,
    size_t blob_size,
    size_t buffer_size,
    int64_t reported_length,
    size_t max_read_retries = 1,
    std::optional<size_t> served_size = {},
    std::optional<size_t> known_object_size = {},
    std::optional<size_t> blob_size_after_first = {},
    const std::string & expected_etag = {},
    bool refuse_range_past_the_data = false)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = std::make_shared<MisbehavingRangeTransport>(
        max_response_size, served_size.value_or(blob_size), blob_size, /* send_etag */ true, reported_length,
        /* ignore_range */ false, ETagBehaviour{}, blob_size_after_first, refuse_range_past_the_data);

    auto container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options), /* blob_prefix */ "");

    DB::ReadSettings read_settings;
    read_settings.remote_fs_settings.buffer_size = buffer_size;

    DB::ReadBufferFromAzureBlobStorage buffer(
        container_client,
        "blob",
        read_settings,
        max_read_retries,
        /* max_single_download_retries */ 1,
        /* use_external_buffer */ false,
        /* restricted_seek */ false,
        /* read_until_position */ 0,
        /* blob_storage_log */ {},
        /* container_for_logging */ {},
        known_object_size,
        expected_etag);

    std::string result;
    DB::readStringUntilEOF(result, buffer);
    return result;
}

/// Reads a whole blob through an endpoint whose object generation behaves as `etags` says, with
/// the read pinned to `expected_etag`.
std::string readPinnedToETag(
    size_t max_response_size,
    size_t blob_size,
    size_t buffer_size,
    const std::string & expected_etag,
    const ETagBehaviour & etags,
    size_t max_read_retries = 4)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = std::make_shared<MisbehavingRangeTransport>(
        max_response_size, blob_size, blob_size, /* send_etag */ true, /* reported_length */ std::nullopt,
        /* ignore_range */ false, etags);

    auto container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options), /* blob_prefix */ "");

    DB::ReadSettings read_settings;
    read_settings.remote_fs_settings.buffer_size = buffer_size;

    DB::ReadBufferFromAzureBlobStorage buffer(
        container_client,
        "blob",
        read_settings,
        max_read_retries,
        /* max_single_download_retries */ 1,
        /* use_external_buffer */ false,
        /* restricted_seek */ false,
        /* read_until_position */ 0,
        /* blob_storage_log */ {},
        /* container_for_logging */ {},
        blob_size,
        expected_etag);

    std::string result;
    DB::readStringUntilEOF(result, buffer);
    return result;
}

/// A positioned-read buffer pinned to `expected_etag` over an endpoint that reports `etags`.
std::unique_ptr<DB::ReadBufferFromAzureBlobStorage> makeFreshBufferPinnedToETag(
    size_t blob_size, const std::string & expected_etag, const ETagBehaviour & etags)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = std::make_shared<MisbehavingRangeTransport>(
        blob_size, blob_size, blob_size, /* send_etag */ true, /* reported_length */ std::nullopt,
        /* ignore_range */ false, etags);

    auto container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options), /* blob_prefix */ "");

    return std::make_unique<DB::ReadBufferFromAzureBlobStorage>(
        container_client,
        "blob",
        DB::ReadSettings{},
        /* max_single_read_retries */ 1,
        /* max_single_download_retries */ 1,
        /* use_external_buffer */ false,
        /* restricted_seek */ false,
        /* read_until_position */ 0,
        /* blob_storage_log */ DB::BlobStorageLogWriterPtr{},
        /* container_for_logging */ std::string{},
        blob_size,
        expected_etag);
}

/// A buffer over an endpoint that serves a blob of `blob_size` bytes, `max_response_size` bytes
/// at a time, without any read having been performed on it yet. With `ignore_range`, the endpoint
/// answers every request with `200 OK` and the object from byte 0. `known_object_size` is the size
/// of the object as it is known locally, from a listing or a `HEAD`, before any read.
std::unique_ptr<DB::ReadBufferFromAzureBlobStorage> makeFreshBuffer(
    size_t max_response_size,
    size_t blob_size,
    bool ignore_range = false,
    std::optional<size_t> known_object_size = {},
    const std::string & expected_etag = {})
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = std::make_shared<MisbehavingRangeTransport>(
        max_response_size, blob_size, blob_size, /* send_etag */ true, /* reported_length */ std::nullopt, ignore_range);

    auto container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options), /* blob_prefix */ "");

    return std::make_unique<DB::ReadBufferFromAzureBlobStorage>(
        container_client,
        "blob",
        DB::ReadSettings{},
        /* max_single_read_retries */ 1,
        /* max_single_download_retries */ 1,
        /* use_external_buffer */ false,
        /* restricted_seek */ false,
        /* read_until_position */ 0,
        /* blob_storage_log */ DB::BlobStorageLogWriterPtr{},
        /* container_for_logging */ std::string{},
        known_object_size,
        expected_etag);
}

/// Copies a blob of `blob_size` bytes through `copyAzureBlobStorageFile` with the native copy
/// disabled, so that the copy goes through the read-and-write fallback, in parts of `part_size`
/// bytes (a single part when `part_size` is at least `blob_size`), from an endpoint that answers
/// every ranged request with at most `max_response_size` bytes and whose object generation behaves
/// as `etags` says. The read is pinned to `expected_etag`. Returns the bytes the endpoint was asked
/// to store as the destination.
std::string copyThroughReadAndWrite(
    size_t blob_size,
    size_t part_size,
    size_t max_response_size,
    const std::string & expected_etag,
    const ETagBehaviour & etags,
    size_t max_read_retries = 4)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        max_response_size, blob_size, blob_size, /* send_etag */ true, /* reported_length */ std::nullopt,
        /* ignore_range */ false, etags);
    client_options.Transport.Transport = transport;

    auto container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options), /* blob_prefix */ "");

    auto settings = std::make_shared<DB::AzureBlobStorage::RequestSettings>();
    settings->use_native_copy = false;
    settings->max_single_read_retries = max_read_retries;
    settings->max_single_download_retries = 1;
    /// A blob of at least `max_single_part_upload_size` bytes is copied in parts of `part_size`.
    settings->max_single_part_upload_size = part_size >= blob_size ? blob_size + 1 : 1;
    settings->min_upload_part_size = part_size;
    settings->max_upload_part_size = part_size;

    DB::ReadSettings read_settings;
    read_settings.remote_fs_settings.buffer_size = 64;

    DB::copyAzureBlobStorageFile(
        container_client,
        container_client,
        /* src_container_for_logging */ "container",
        /* src_blob */ "blob",
        /* src_size */ blob_size,
        expected_etag,
        /* dest_container_for_logging */ "container",
        /* dest_blob */ "copy",
        settings,
        read_settings,
        /* object_to_attributes */ std::nullopt);

    return transport->uploadedData();
}

/// What a native (server-side) blob-to-blob copy left behind at the endpoint.
struct NativeCopyOutcome
{
    /// The generation of the source that each native copy request transferred.
    std::vector<std::string> copied_generations;
    /// The `x-ms-source-if-match` header of each native copy request, empty when it carried none.
    std::vector<std::string> source_if_match_headers;
    /// Bytes the endpoint was asked to store through the read-and-write fallback, expected empty.
    std::string uploaded;
};

/// Copies a `blob_size`-byte blob with the native copy enabled, pinned to `expected_etag`, against
/// an endpoint whose object generation behaves as `etags` says. With `asynchronous`, the copy is
/// the asynchronous `Copy Blob` (`StartCopyFromUri`, polled to completion) rather than the
/// synchronous `Copy Blob From URL` (`CopyFromUri`).
NativeCopyOutcome copyNatively(
    size_t blob_size, const std::string & expected_etag, const ETagBehaviour & etags, bool asynchronous)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        blob_size, blob_size, blob_size, /* send_etag */ true, /* reported_length */ std::nullopt,
        /* ignore_range */ false, etags);
    client_options.Transport.Transport = transport;

    auto container_client = std::make_shared<const DB::AzureBlobStorage::ContainerClient>(
        Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options), /* blob_prefix */ "");

    auto settings = std::make_shared<DB::AzureBlobStorage::RequestSettings>();
    settings->use_native_copy = true;
    /// A blob of at least `max_single_part_copy_size` bytes is copied asynchronously.
    settings->max_single_part_copy_size = asynchronous ? blob_size : blob_size + 1;
    settings->max_single_read_retries = 1;
    settings->max_single_download_retries = 1;

    DB::copyAzureBlobStorageFile(
        container_client,
        container_client,
        /* src_container_for_logging */ "container",
        /* src_blob */ "blob",
        /* src_size */ blob_size,
        expected_etag,
        /* dest_container_for_logging */ "container",
        /* dest_blob */ "copy",
        settings,
        DB::ReadSettings{},
        /* object_to_attributes */ std::nullopt);

    return NativeCopyOutcome{
        .copied_generations = transport->nativelyCopiedGenerations(),
        .source_if_match_headers = transport->sourceIfMatchHeaders(),
        .uploaded = transport->uploadedData()};
}

/// A shared endpoint (`transport`) seen through a fresh client, so that several clients of one test
/// (the one a copy is driven through, the one an object storage deletes through) observe the same
/// object and the same overwrite.
Azure::Storage::Blobs::BlobContainerClient blobContainerClientOver(const std::shared_ptr<MisbehavingRangeTransport> & transport)
{
    Azure::Storage::Blobs::BlobClientOptions client_options;
    client_options.Retry.MaxRetries = 0;
    client_options.Transport.Transport = transport;
    return Azure::Storage::Blobs::BlobContainerClient("http://azure.invalid/container", client_options);
}

std::shared_ptr<DB::AzureBlobStorage::ContainerClient> containerClientOver(const std::shared_ptr<MisbehavingRangeTransport> & transport)
{
    return std::make_shared<DB::AzureBlobStorage::ContainerClient>(blobContainerClientOver(transport), /* blob_prefix */ "");
}

/// An `AzureObjectStorage` over the shared endpoint `transport`.
std::unique_ptr<DB::AzureObjectStorage> objectStorageOver(const std::shared_ptr<MisbehavingRangeTransport> & transport)
{
    /// The delete path creates a `BlobStorageLogWriter`, which looks the log up in the global context.
    getContext();

    return std::make_unique<DB::AzureObjectStorage>(
        "azure",
        DB::AzureBlobStorage::AuthMethod{DB::AzureBlobStorage::ConnectionString{""}},
        std::make_unique<DB::AzureBlobStorage::ContainerClient>(blobContainerClientOver(transport), /* blob_prefix */ ""),
        std::make_unique<DB::AzureBlobStorage::RequestSettings>(),
        DB::AzureBlobStorage::ConnectionParams{},
        /* object_namespace */ "container",
        /* description */ "http://azure.invalid/container",
        /* common_key_prefix */ "");
}

/// A `StoredObject` for the blob `blob`, pinned to the generation `etag` (unpinned when empty).
DB::StoredObject blobGeneration(const std::string & etag)
{
    DB::StoredObject object("blob");
    object.etag = etag;
    return object;
}

/// What a delete left behind at the endpoint.
struct DeleteOutcome
{
    /// The generation that each successful `Delete Blob` removed.
    std::vector<std::string> deleted_generations;
    /// The `If-Match` header of each `Delete Blob` request, empty when it carried none.
    std::vector<std::string> if_match_headers;
};

/// Deletes the blob through an `AzureObjectStorage`, pinned to `expected_etag`, against an endpoint
/// whose object generation behaves as `etags` says.
DeleteOutcome deleteThroughObjectStorage(const std::string & expected_etag, const ETagBehaviour & etags)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        100, 100, 100, /* send_etag */ true, /* reported_length */ std::nullopt, /* ignore_range */ false, etags);

    objectStorageOver(transport)->removeObjectIfExists(blobGeneration(expected_etag));

    return DeleteOutcome{.deleted_generations = transport->deletedGenerations(), .if_match_headers = transport->deleteIfMatchHeaders()};
}

/// What an Azure `MOVE` (the `after_processing = 'move'` step of `ObjectStorageQueue`) left behind
/// at the endpoint.
struct MoveOutcome
{
    /// The generation of the source that the copy transferred.
    std::vector<std::string> copied_generations;
    /// The generation that each successful `Delete Blob` removed.
    std::vector<std::string> deleted_generations;
    /// The error code the copy failed with, if it did (the delete is then not attempted).
    std::optional<int> copy_error_code;
    /// The error code the delete failed with, if it did.
    std::optional<int> delete_error_code;
};

/// Drives the sequence of an Azure `MOVE` the way `ObjectStorageQueuePostProcessor::moveAzureBlobs`
/// does it for a source whose ingested generation is `ingested_etag` (size 100, both recorded from
/// the listing entry the reader was opened on): copy that generation (natively, pinned to its
/// `ETag`), then delete the same generation through the object storage; the endpoint holds
/// `generation_at_endpoint` when the move starts. With `overwrite_between_copy_and_delete`, the
/// source is overwritten by somebody else after the copy has completed and before the delete.
MoveOutcome moveBlob(const std::string & ingested_etag, const std::string & generation_at_endpoint, bool overwrite_between_copy_and_delete)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        100, 100, 100, /* send_etag */ true, /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = generation_at_endpoint, .etag_after_first = "", .honour_if_match = true});
    auto src_client = containerClientOver(transport);
    auto object_storage = objectStorageOver(transport);

    auto settings = std::make_shared<DB::AzureBlobStorage::RequestSettings>();
    settings->use_native_copy = true;
    settings->max_single_part_copy_size = 101;
    settings->max_single_read_retries = 1;
    settings->max_single_download_retries = 1;

    std::optional<int> copy_error_code;
    try
    {
        DB::copyAzureBlobStorageFile(
            src_client,
            src_client,
            /* src_container_for_logging */ "container",
            /* src_blob */ "blob",
            /* src_size */ 100,
            ingested_etag,
            /* dest_container_for_logging */ "container",
            /* dest_blob */ "moved/blob",
            settings,
            DB::ReadSettings{},
            /* object_to_attributes */ std::nullopt);
    }
    catch (const DB::Exception & e)
    {
        copy_error_code = e.code();
    }

    if (copy_error_code)
        return MoveOutcome{
            .copied_generations = transport->nativelyCopiedGenerations(),
            .deleted_generations = transport->deletedGenerations(),
            .copy_error_code = copy_error_code,
            .delete_error_code = std::nullopt};

    if (overwrite_between_copy_and_delete)
        transport->overwriteObject(ETagBehaviour::second_generation);

    std::optional<int> delete_error_code;
    try
    {
        object_storage->removeObjectIfExists(blobGeneration(ingested_etag));
    }
    catch (const DB::Exception & e)
    {
        delete_error_code = e.code();
    }

    return MoveOutcome{
        .copied_generations = transport->nativelyCopiedGenerations(),
        .deleted_generations = transport->deletedGenerations(),
        .copy_error_code = std::nullopt,
        .delete_error_code = delete_error_code};
}

/// What `AzureObjectStorage::copyObject` (the same-container `MOVE` of `ObjectStorageQueue`, which
/// copies through a read and a write) left behind at the endpoint.
struct CopyObjectOutcome
{
    /// The bytes the copy uploaded to the destination.
    std::string copied;
    /// The error code the copy failed with, if it did.
    std::optional<int> error_code;
};

/// Copies the blob `blob` (100 bytes) through `AzureObjectStorage::copyObject` from a source
/// `StoredObject` that carries the generation `caller_etag` (unpinned when empty) and the size
/// `caller_size`, while the endpoint holds `generation_at_endpoint` (and reports it only when
/// `send_etag`).
CopyObjectOutcome copyObjectThroughObjectStorage(
    const std::string & caller_etag, uint64_t caller_size, const std::string & generation_at_endpoint, bool send_etag = true)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        100, 100, 100, send_etag, /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = generation_at_endpoint, .etag_after_first = "", .honour_if_match = true});

    DB::StoredObject object_from("blob", /* local_path */ "", caller_size);
    object_from.etag = caller_etag;

    std::optional<int> error_code;
    try
    {
        objectStorageOver(transport)->copyObject(object_from, DB::StoredObject("moved/blob"), DB::ReadSettings{}, DB::WriteSettings{});
    }
    catch (const DB::Exception & e)
    {
        error_code = e.code();
    }

    return CopyObjectOutcome{.copied = transport->uploadedData(), .error_code = error_code};
}

/// What the move of a file of a `plain_rewritable` disk left behind at the endpoint.
struct PlainRewritableMoveOutcome
{
    /// The bytes of the source that the copy stored at the destination. A copy between two clients
    /// of this endpoint goes through a read and a write, so this is what says whether the copy
    /// happened, and the `If-Match` of its `GET` is what pins it to one generation.
    std::string copied;
    /// The generation that each successful `Delete Blob` removed.
    std::vector<std::string> deleted_generations;
    /// The error code the move failed with, if it did.
    std::optional<int> error_code;
};

/// Drives the sequence a `plain_rewritable` metadata operation performs when it moves a file: name
/// the generation of the source blob (`pinToTheGenerationThatIsThereNow`, the production helper),
/// copy that generation aside, then delete the source. With `overwrite_between_copy_and_delete`,
/// somebody else overwrites the source blob after the copy and before the delete. With `pin`
/// disabled, the copy and the delete address the blob by path only, the way the operation did
/// before it pinned them - the endpoint holds `send_etag` and honours `If-Match` in both cases.
/// With `blob_missing`, the endpoint answers the `HEAD` that names the generation with `404`.
PlainRewritableMoveOutcome movePlainRewritableFile(
    bool overwrite_between_copy_and_delete, bool pin = true, bool send_etag = true, bool blob_missing = false)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        100, 100, 100, send_etag, /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true},
        /* blob_size_after_first */ std::nullopt, /* refuse_range_past_the_data */ false, blob_missing);
    auto object_storage = objectStorageOver(transport);

    std::optional<int> error_code;
    try
    {
        const DB::StoredObject source
            = pin ? DB::pinToTheGenerationThatIsThereNow(*object_storage, "blob") : DB::StoredObject("blob");

        object_storage->copyObject(source, DB::StoredObject("moved/blob"), DB::ReadSettings{}, DB::WriteSettings{});

        if (overwrite_between_copy_and_delete)
            transport->overwriteObject(ETagBehaviour::second_generation);

        object_storage->removeObjectIfExists(source);
    }
    catch (const DB::Exception & e)
    {
        error_code = e.code();
    }

    return PlainRewritableMoveOutcome{
        .copied = transport->uploadedData(),
        .deleted_generations = transport->deletedGenerations(),
        .error_code = error_code};
}

}

/// The endpoint returns more data than was requested: the copy must be capped at the size of
/// the destination buffer instead of overflowing it.
TEST(AzureBodyStreamCopy, OverlongResponse)
{
    const size_t requested = 8;
    LyingBodyStream body_stream(std::vector<uint8_t>(1024, 0xAB), 1024);

    std::vector<char> destination(requested);
    const size_t copied = DB::copyFromAzureBodyStream(body_stream, destination.data(), requested, Azure::Core::Context());

    ASSERT_EQ(copied, requested);
    for (char byte : destination)
        ASSERT_EQ(static_cast<uint8_t>(byte), 0xAB);
}

/// The endpoint reports a large `Content-Length` but sends fewer bytes: only the received bytes
/// are copied and reported.
TEST(AzureBodyStreamCopy, ShortResponse)
{
    const size_t requested = 8;
    LyingBodyStream body_stream(std::vector<uint8_t>(3, 0xCD), 1024);

    std::vector<char> destination(requested);
    const size_t copied = DB::copyFromAzureBodyStream(body_stream, destination.data(), requested, Azure::Core::Context());

    ASSERT_EQ(copied, 3);
}

/// The endpoint reports a `Content-Length` smaller than the bytes the body can actually produce:
/// the reported length must not truncate the copy, because the requested size is the only bound
/// that matters and the copy stops at the actual end of the body anyway.
TEST(AzureBodyStreamCopy, LengthSmallerThanData)
{
    const size_t requested = 8;
    LyingBodyStream body_stream(std::vector<uint8_t>(8, 0x5A), 1);

    std::vector<char> destination(requested);
    const size_t copied = DB::copyFromAzureBodyStream(body_stream, destination.data(), requested, Azure::Core::Context());

    ASSERT_EQ(copied, requested);
    for (char byte : destination)
        ASSERT_EQ(static_cast<uint8_t>(byte), 0x5A);
}

/// A stream of unknown length must not be copied beyond the destination buffer either.
TEST(AzureBodyStreamCopy, UnknownLength)
{
    const size_t requested = 4;
    LyingBodyStream body_stream(std::vector<uint8_t>(64, 0xEF), -1);

    std::vector<char> destination(requested);
    const size_t copied = DB::copyFromAzureBodyStream(body_stream, destination.data(), requested, Azure::Core::Context());

    ASSERT_EQ(copied, requested);
}

/// The endpoint answers a 100-byte ranged request with 128 bytes. The reader must stop at the right
/// bound instead of handing the extra 28 bytes to the caller: it reads through a 64-byte buffer, so
/// without a locally derived bound the second `nextImpl` call would already deliver bytes 100..127.
TEST(AzureReadUntilPosition, OverlongRangeResponse)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithRightBound(
        /* max_response_size */ 128, /* served_size */ 1000, /* blob_size */ 1000, /* read_until_position */ 100, /* buffer_size */ 64));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The same, with a reading buffer larger than the requested range: a single response must not
/// overrun the right bound either.
TEST(AzureReadUntilPosition, OverlongRangeResponseWithLargeBuffer)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithRightBound(
        /* max_response_size */ 128, /* served_size */ 1000, /* blob_size */ 1000, /* read_until_position */ 100, /* buffer_size */ 1024));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// A well-behaved endpoint returns exactly the requested range.
TEST(AzureReadUntilPosition, ExactRangeResponse)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithRightBound(
        /* max_response_size */ 100, /* served_size */ 1000, /* blob_size */ 1000, /* read_until_position */ 100, /* buffer_size */ 64));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The endpoint answers every ranged request with at most 40 bytes, less than was requested. The
/// right bound is authoritative: the reader must reopen the download at the new offset and hand
/// the caller all 100 requested bytes instead of reporting the end of the file at 40 bytes.
TEST(AzureReadUntilPosition, ShortRangeResponse)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithRightBound(
        /* max_response_size */ 40, /* served_size */ 1000, /* blob_size */ 1000, /* read_until_position */ 100, /* buffer_size */ 64,
        /* max_read_retries */ 4));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The blob ends at 40 bytes no matter how often the download is reopened, while the caller asked
/// to read until position 100. A bounded read must either reach the right bound or fail - it must
/// not silently report the end of the file before the bound.
TEST(AzureReadUntilPosition, TruncatedBlob)
{
    try
    {
        readWithRightBound(
            /* max_response_size */ 40, /* served_size */ 40, /* blob_size */ 1000, /* read_until_position */ 100, /* buffer_size */ 64,
            /* max_read_retries */ 3);
        FAIL() << "Expected an exception on a premature end of the response before the right bound";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::UNEXPECTED_END_OF_FILE);
    }
}

/// `supportsReadAt` promises that a positioned read may be performed on a buffer on which nothing
/// has been read yet, so `readBigAt` must not depend on any state that only the sequential path or
/// `tryGetFileSize` creates.
TEST(AzureReadBigAt, OnFreshBuffer)
{
    auto buffer = makeFreshBuffer(/* max_response_size */ 100, /* blob_size */ 100);
    ASSERT_TRUE(buffer->supportsReadAt());

    std::string destination(16, '\0');
    size_t bytes_read = 0;
    ASSERT_NO_THROW(bytes_read = buffer->readBigAt(destination.data(), destination.size(), /* range_begin */ 0, {}));

    ASSERT_EQ(bytes_read, destination.size());
    assertCountsUpFromZero(destination);
}

/// The object is known locally to be 100 bytes long, but the endpoint holds 128 bytes and answers
/// a positioned read of bytes 96..111 with all 16 of them (`206 bytes 96-111/128`). Only the 4
/// bytes before the locally known end of the object exist, so only those may reach the caller: the
/// local size bounds a positioned read as much as a sequential one.
TEST(AzureReadBigAt, OverlongResponseCrossingKnownEndOfObject)
{
    auto buffer = makeFreshBuffer(
        /* max_response_size */ 128, /* blob_size */ 128, /* ignore_range */ false, /* known_object_size */ 100,
        /* expected_etag */ ETagBehaviour::first_generation_bare);

    std::string destination(16, '\xAB');
    size_t bytes_read = 0;
    ASSERT_NO_THROW(bytes_read = buffer->readBigAt(destination.data(), destination.size(), /* range_begin */ 96, {}));

    ASSERT_EQ(bytes_read, 4u);
    for (size_t i = 0; i < bytes_read; ++i)
        ASSERT_EQ(static_cast<uint8_t>(destination[i]), static_cast<uint8_t>(96 + i)) << "at position " << i;
    /// The tail of the destination is not initialized by the read, and the caller must be told so.
    for (size_t i = bytes_read; i < destination.size(); ++i)
        ASSERT_EQ(static_cast<uint8_t>(destination[i]), 0xAB) << "at position " << i;
}

/// A positioned read that starts at or past the locally known end of the object is the end of the
/// file, whatever the endpoint would be willing to serve there.
TEST(AzureReadBigAt, StartsPastKnownEndOfObject)
{
    auto buffer = makeFreshBuffer(
        /* max_response_size */ 128, /* blob_size */ 128, /* ignore_range */ false, /* known_object_size */ 100,
        /* expected_etag */ ETagBehaviour::first_generation_bare);

    std::string destination(16, '\xAB');
    size_t bytes_read = 16;
    ASSERT_NO_THROW(bytes_read = buffer->readBigAt(destination.data(), destination.size(), /* range_begin */ 100, {}));
    ASSERT_EQ(bytes_read, 0u);

    bytes_read = 16;
    ASSERT_NO_THROW(bytes_read = buffer->readBigAt(destination.data(), destination.size(), /* range_begin */ 120, {}));
    ASSERT_EQ(bytes_read, 0u);
}

/// A positioned read that stays within the locally known object is not affected by the bound.
TEST(AzureReadBigAt, WithinKnownObject)
{
    auto buffer = makeFreshBuffer(
        /* max_response_size */ 128, /* blob_size */ 128, /* ignore_range */ false, /* known_object_size */ 100,
        /* expected_etag */ ETagBehaviour::first_generation_bare);

    std::string destination(16, '\0');
    size_t bytes_read = 0;
    ASSERT_NO_THROW(bytes_read = buffer->readBigAt(destination.data(), destination.size(), /* range_begin */ 0, {}));

    ASSERT_EQ(bytes_read, destination.size());
    assertCountsUpFromZero(destination);
}

/// The `ETag` response header is optional, and `Azure::ETag::ToString` aborts the process when the
/// tag is absent, so an endpoint that does not send one must not take the server down with it.
TEST(AzureReadUntilPosition, ResponseWithoutETag)
{
    std::string data;
    ASSERT_NO_THROW(
        data = readWithRightBound(
            /* max_response_size */ 100, /* served_size */ 1000, /* blob_size */ 1000, /* read_until_position */ 100, /* buffer_size */ 64,
            /* max_read_retries */ 1, /* send_etag */ false));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// An unbounded sequential read of an endpoint that under-reports the `Content-Length` of its
/// response: the reported length must not become the end of the file, otherwise a broken endpoint
/// can silently truncate the data. The whole 100-byte body of the response must be delivered even
/// though it claims to be a single byte long.
TEST(AzureReadWithoutRightBound, LengthSmallerThanResponse)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithoutRightBound(
        /* max_response_size */ 100, /* blob_size */ 100, /* buffer_size */ 64, /* reported_length */ 1));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// An endpoint that reports a length larger than the data it sends must not make the reader hang
/// or report more data than it received: the end of the body is the end of the file.
TEST(AzureReadWithoutRightBound, LengthLargerThanBlob)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithoutRightBound(
        /* max_response_size */ 100, /* blob_size */ 100, /* buffer_size */ 64, /* reported_length */ 1000));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// An unbounded read of an endpoint that caps every open-ended request to 40 bytes of a 100-byte
/// object. The end of a response body is not the end of the file: the reader must reopen the
/// download at the offset it reached and reassemble the whole object, instead of reporting the end
/// of the file after the first response.
TEST(AzureReadWithoutRightBound, ResponseShorterThanObject)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithoutRightBound(
        /* max_response_size */ 40, /* blob_size */ 100, /* buffer_size */ 64, /* reported_length */ 40,
        /* max_read_retries */ 4));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The same endpoint, except that it never serves a byte at position 40 or beyond while still
/// advertising a 100-byte object. Reopening the download does not help, so the read must fail
/// rather than hand the caller a silently truncated file.
TEST(AzureReadWithoutRightBound, TruncatedObject)
{
    try
    {
        readWithoutRightBound(
            /* max_response_size */ 40, /* blob_size */ 100, /* buffer_size */ 64, /* reported_length */ 40,
            /* max_read_retries */ 3, /* served_size */ 40);
        FAIL() << "Expected an exception on a response that ends before the advertised size of the object";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::UNEXPECTED_END_OF_FILE);
    }
}

/// The size of the object is not known locally, and the endpoint's idea of it shrinks between the
/// requests of one read: the first response advertises `bytes 0-39/1000`, the reopen after it
/// advertises `bytes 40-79/80`, and nothing is served from byte 80 on. The lower bound learnt from
/// the first response must stand - the object was declared to be at least 1000 bytes long - so the
/// end of the second response at byte 80 is a premature end of the response and the read must
/// fail rather than silently return 80 bytes as the whole file.
TEST(AzureReadWithoutRightBound, ShrinkingReportedObjectSize)
{
    try
    {
        readWithoutRightBound(
            /* max_response_size */ 40, /* blob_size */ 1000, /* buffer_size */ 64, /* reported_length */ 40,
            /* max_read_retries */ 3, /* served_size */ 80, /* known_object_size */ std::nullopt, /* blob_size_after_first */ 80);
        FAIL() << "Expected an exception when a later response advertises a smaller object than an earlier one did";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::UNEXPECTED_END_OF_FILE);
    }
}

/// The endpoint caps every open-ended request to 40 bytes and reports the object as 40 bytes long
/// in the `Content-Range` of that very response, while it does hold - and will serve, when asked
/// again - all 100 bytes. The total advertised by one response is not the end of the file: the read
/// must reopen the download at the offset it reached and reassemble the whole object. The retry
/// budget is left at one to pin down that confirming the end of the object is a normal request and
/// does not spend it.
TEST(AzureReadWithoutRightBound, UnderReportedObjectSize)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithoutRightBound(
        /* max_response_size */ 40, /* blob_size */ 40, /* buffer_size */ 64, /* reported_length */ 40,
        /* max_read_retries */ 1, /* served_size */ 100));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The same read against an endpoint that answers a range past the end of the blob the way a real
/// one does, with `416 Range Not Satisfiable`. That refusal is the endpoint stating that the object
/// has no byte at that offset, so it ends the read instead of failing it.
TEST(AzureReadWithoutRightBound, RangePastTheObjectRefused)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithoutRightBound(
        /* max_response_size */ 40, /* blob_size */ 100, /* buffer_size */ 64, /* reported_length */ 40,
        /* max_read_retries */ 4, /* served_size */ 100, /* known_object_size */ std::nullopt,
        /* blob_size_after_first */ std::nullopt, /* expected_etag */ {}, /* refuse_range_past_the_data */ true));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// An empty blob whose size is not known locally: the first request is already a range that the
/// endpoint has nothing to answer, and a blob with no bytes is refused with `400 Bad Request` /
/// `InvalidRange` rather than `416 Range Not Satisfiable`. That is the end of the file, not an
/// error - a directory marker of a `plain_rewritable` metadata storage is read exactly like this.
TEST(AzureReadWithoutRightBound, EmptyObjectOfUnknownSize)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithoutRightBound(
        /* max_response_size */ 100, /* blob_size */ 0, /* buffer_size */ 64, /* reported_length */ 0,
        /* max_read_retries */ 1, /* served_size */ 0, /* known_object_size */ std::nullopt,
        /* blob_size_after_first */ std::nullopt, /* expected_etag */ {}, /* refuse_range_past_the_data */ true));

    ASSERT_TRUE(data.empty());
}

/// A refused range inside the object the endpoint itself advertised is not the end of the file: the
/// endpoint claims a 100-byte object and refuses a range at byte 40, so the read must fail instead
/// of accepting the refusal as the end of a 40-byte file.
TEST(AzureReadWithoutRightBound, RangeInsideTheObjectRefused)
{
    try
    {
        readWithoutRightBound(
            /* max_response_size */ 40, /* blob_size */ 100, /* buffer_size */ 64, /* reported_length */ 40,
            /* max_read_retries */ 3, /* served_size */ 40, /* known_object_size */ std::nullopt,
            /* blob_size_after_first */ std::nullopt, /* expected_etag */ {}, /* refuse_range_past_the_data */ true);
        FAIL() << "Expected an exception on a refused range inside the advertised size of the object";
    }
    catch (const Azure::Core::RequestFailedException & e)
    {
        ASSERT_EQ(e.StatusCode, Azure::Core::Http::HttpStatusCode::RangeNotSatisfiable);
    }
}

/// `readBigAt` asks for bytes 40..55, but the endpoint ignores the range and answers `200 OK` with
/// the whole object from byte 0. Consuming that body would hand the caller bytes 0..15 under the
/// offsets 40..55, so the read must fail instead.
TEST(AzureReadBigAt, RangeIgnoredByEndpoint)
{
    auto buffer = makeFreshBuffer(/* max_response_size */ 100, /* blob_size */ 100, /* ignore_range */ true);

    std::string destination(16, '\0');
    try
    {
        buffer->readBigAt(destination.data(), destination.size(), /* range_begin */ 40, {});
        FAIL() << "Expected an exception on a full-object response to a positioned read at a nonzero offset";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::HTTP_RANGE_NOT_SATISFIABLE);
    }
}

/// A `200 OK` response with the whole object is a correct answer to a positioned read that starts
/// at byte 0, so it must be accepted there.
TEST(AzureReadBigAt, FullObjectResponseAtZero)
{
    auto buffer = makeFreshBuffer(/* max_response_size */ 100, /* blob_size */ 100, /* ignore_range */ true);

    std::string destination(16, '\0');
    size_t bytes_read = 0;
    ASSERT_NO_THROW(bytes_read = buffer->readBigAt(destination.data(), destination.size(), /* range_begin */ 0, {}));

    ASSERT_EQ(bytes_read, destination.size());
    assertCountsUpFromZero(destination);
}

/// A sequential read that starts at byte 40 after a seek, against an endpoint that ignores the
/// range and answers `200 OK` with the object from byte 0: the reader must not advance as though
/// the bytes it received belonged to offset 40.
TEST(AzureSequentialRead, RangeIgnoredAfterSeek)
{
    auto buffer = makeFreshBuffer(/* max_response_size */ 100, /* blob_size */ 100, /* ignore_range */ true);
    buffer->seek(40, SEEK_SET);

    try
    {
        buffer->next();
        FAIL() << "Expected an exception on a full-object response to a sequential read at a nonzero offset";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::HTTP_RANGE_NOT_SATISFIABLE);
    }
}

/// The endpoint honours nothing about the range: it answers every request with `200 OK` and at
/// most 40 bytes of the object from byte 0. The first response is a correct answer to the request
/// that started at byte 0, but when the reader reopens the download at offset 40 it gets bytes
/// 0..39 again. Those must not be delivered as bytes 40..79: the read must fail.
TEST(AzureReadUntilPosition, RangeIgnoredOnReopen)
{
    try
    {
        readWithRightBound(
            /* max_response_size */ 40, /* served_size */ 1000, /* blob_size */ 1000, /* read_until_position */ 100, /* buffer_size */ 64,
            /* max_read_retries */ 4, /* send_etag */ true, /* ignore_range */ true);
        FAIL() << "Expected an exception on a full-object response to a reopened download at a nonzero offset";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::HTTP_RANGE_NOT_SATISFIABLE);
    }
}

/// The size of the object is known locally, from the `LIST` or `HEAD` that produced the
/// `StoredObject`, while the endpoint caps every open-ended request to 40 bytes and claims in its
/// `Content-Range` that the whole object is 40 bytes long. The locally known size wins: the reader
/// must reopen the download and reassemble all 100 bytes instead of accepting the size that the
/// very response it is validating advertises.
TEST(AzureReadWithoutRightBound, KnownSizeShortFirstResponse)
{
    std::string data;
    ASSERT_NO_THROW(data = readWithoutRightBound(
        /* max_response_size */ 40, /* blob_size */ 40, /* buffer_size */ 64, /* reported_length */ 40,
        /* max_read_retries */ 4, /* served_size */ 100, /* known_object_size */ 100,
        /* blob_size_after_first */ std::nullopt, /* expected_etag */ ETagBehaviour::first_generation_bare));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The same, except that the endpoint really has only 40 bytes to serve while the size known
/// locally is 100. Reopening does not help, and the read must fail rather than report a file that
/// is shorter than the caller already knows it to be.
TEST(AzureReadWithoutRightBound, KnownSizeTruncatedObject)
{
    try
    {
        readWithoutRightBound(
            /* max_response_size */ 40, /* blob_size */ 40, /* buffer_size */ 64, /* reported_length */ 40,
            /* max_read_retries */ 3, /* served_size */ 40, /* known_object_size */ 100,
            /* blob_size_after_first */ std::nullopt, /* expected_etag */ ETagBehaviour::first_generation_bare);
        FAIL() << "Expected an exception on a response that ends before the locally known size of the object";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::UNEXPECTED_END_OF_FILE);
    }
}

/// A read of an object whose generation does not change must not be disturbed by the `If-Match`
/// precondition or by the check of the `ETag` of the response.
TEST(AzureReadPinnedToETag, UnchangedObject)
{
    std::string data;
    ASSERT_NO_THROW(data = readPinnedToETag(
        /* max_response_size */ 40, /* blob_size */ 100, /* buffer_size */ 64,
        ETagBehaviour::first_generation, ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true}));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The blob is overwritten in place after the first response, and the endpoint evaluates
/// `If-Match`, so the reopened download is rejected with `412 Precondition Failed`. That must be
/// reported as the object having changed - and not retried, since it never becomes true again.
TEST(AzureReadPinnedToETag, PreconditionFailedOnReopen)
{
    try
    {
        readPinnedToETag(
            /* max_response_size */ 40, /* blob_size */ 100, /* buffer_size */ 64,
            ETagBehaviour::first_generation,
            ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = ETagBehaviour::second_generation, .honour_if_match = true});
        FAIL() << "Expected an exception on a blob overwritten between two requests of one read";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::FILE_CHANGED_DURING_READ);
    }
}

/// The same overwrite, against an endpoint that ignores `If-Match` and serves the new generation
/// anyway. The `ETag` of the response must still be compared with the expected one, otherwise the
/// caller receives one logical file stitched together from two generations of the blob.
TEST(AzureReadPinnedToETag, ETagChangedOnReopen)
{
    try
    {
        readPinnedToETag(
            /* max_response_size */ 40, /* blob_size */ 100, /* buffer_size */ 64,
            ETagBehaviour::first_generation,
            ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = ETagBehaviour::second_generation, .honour_if_match = false});
        FAIL() << "Expected an exception on a response that carries a different ETag than the read is pinned to";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::FILE_CHANGED_DURING_READ);
    }
}

/// The expected tag normally comes from a blob listing, which spells it bare, while the endpoint
/// spells the same tag quoted in the `ETag` header and expects the quoted form in `If-Match`. The
/// two spellings of one tag must be recognized as the same generation: the `If-Match` sent must
/// satisfy an endpoint that evaluates it literally, and the response tag must not be mistaken for
/// a change of the object.
TEST(AzureReadPinnedToETag, BareExpectedETag)
{
    std::string data;
    ASSERT_NO_THROW(data = readPinnedToETag(
        /* max_response_size */ 40, /* blob_size */ 100, /* buffer_size */ 64,
        ETagBehaviour::first_generation_bare, ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true}));

    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The difference in spelling must not hide a real change of the object either.
TEST(AzureReadPinnedToETag, BareExpectedETagChangedOnReopen)
{
    try
    {
        readPinnedToETag(
            /* max_response_size */ 40, /* blob_size */ 100, /* buffer_size */ 64,
            ETagBehaviour::first_generation_bare,
            ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = ETagBehaviour::second_generation, .honour_if_match = false});
        FAIL() << "Expected an exception on a response that carries a different ETag than the read is pinned to";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::FILE_CHANGED_DURING_READ);
    }
}

/// A positioned read is pinned to the generation of the object as well: `readBigAt` is used for
/// column chunks of the same file, and mixing generations between them is just as wrong.
TEST(AzureReadBigAt, ETagChanged)
{
    auto buffer = makeFreshBufferPinnedToETag(
        /* blob_size */ 100, ETagBehaviour::first_generation, ETagBehaviour{.etag = ETagBehaviour::second_generation, .etag_after_first = "", .honour_if_match = false});

    std::string destination(16, '\0');
    try
    {
        buffer->readBigAt(destination.data(), destination.size(), /* range_begin */ 0, {});
        FAIL() << "Expected an exception on a positioned read of a different generation of the blob";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::FILE_CHANGED_DURING_READ);
    }
}

/// A positioned read of the generation it is pinned to must succeed.
TEST(AzureReadBigAt, ETagUnchanged)
{
    auto buffer = makeFreshBufferPinnedToETag(
        /* blob_size */ 100, ETagBehaviour::first_generation, ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true});

    std::string destination(16, '\0');
    size_t bytes_read = 0;
    ASSERT_NO_THROW(bytes_read = buffer->readBigAt(destination.data(), destination.size(), /* range_begin */ 0, {}));

    ASSERT_EQ(bytes_read, destination.size());
    assertCountsUpFromZero(destination);
}

/// The same for a positioned read pinned to a tag in the bare spelling of a listing.
TEST(AzureReadBigAt, BareExpectedETag)
{
    auto buffer = makeFreshBufferPinnedToETag(
        /* blob_size */ 100, ETagBehaviour::first_generation_bare, ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true});

    std::string destination(16, '\0');
    size_t bytes_read = 0;
    ASSERT_NO_THROW(bytes_read = buffer->readBigAt(destination.data(), destination.size(), /* range_begin */ 0, {}));

    ASSERT_EQ(bytes_read, destination.size());
    assertCountsUpFromZero(destination);
}

/// The read-and-write fallback of a blob-to-blob copy, in several parts, against an endpoint that
/// answers every request with fewer bytes than the part asks for: every part must be reassembled
/// from several responses, and the destination must be the whole source, byte for byte.
TEST(AzureCopyThroughReadAndWrite, MultipartUnchangedObject)
{
    std::string copied;
    ASSERT_NO_THROW(copied = copyThroughReadAndWrite(
        /* blob_size */ 100, /* part_size */ 40, /* max_response_size */ 30,
        ETagBehaviour::first_generation, ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true}));

    ASSERT_EQ(copied.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(copied);
}

/// The same copy in a single part.
TEST(AzureCopyThroughReadAndWrite, SinglepartUnchangedObject)
{
    std::string copied;
    ASSERT_NO_THROW(copied = copyThroughReadAndWrite(
        /* blob_size */ 100, /* part_size */ 100, /* max_response_size */ 30,
        ETagBehaviour::first_generation, ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true}));

    ASSERT_EQ(copied.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(copied);
}

/// The source blob is overwritten in place after the first part has been read. Every part opens
/// its own download of the source, so without pinning the destination would be assembled from two
/// generations of the source. The endpoint evaluates `If-Match`, so the second part's download is
/// rejected with `412 Precondition Failed`, which must surface as the object having changed.
TEST(AzureCopyThroughReadAndWrite, PreconditionFailedBetweenParts)
{
    try
    {
        copyThroughReadAndWrite(
            /* blob_size */ 100, /* part_size */ 40, /* max_response_size */ 40,
            ETagBehaviour::first_generation,
            ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = ETagBehaviour::second_generation, .honour_if_match = true});
        FAIL() << "Expected an exception on a source blob overwritten between two parts of one copy";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::FILE_CHANGED_DURING_READ);
    }
}

/// The same overwrite between parts, against an endpoint that ignores `If-Match` and serves the
/// new generation anyway: the `ETag` of the response must still be compared with the one the copy
/// is pinned to.
TEST(AzureCopyThroughReadAndWrite, ETagChangedBetweenParts)
{
    try
    {
        copyThroughReadAndWrite(
            /* blob_size */ 100, /* part_size */ 40, /* max_response_size */ 40,
            ETagBehaviour::first_generation,
            ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = ETagBehaviour::second_generation, .honour_if_match = false});
        FAIL() << "Expected an exception on a part served from a different generation of the source than the copy is pinned to";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::FILE_CHANGED_DURING_READ);
    }
}

/// The overwrite happens within a single part: the endpoint answers with fewer bytes than the part
/// asks for, so the read is reopened at the current offset, and the reopened download hits the
/// new generation of the source.
TEST(AzureCopyThroughReadAndWrite, ETagChangedOnReopenWithinPart)
{
    try
    {
        copyThroughReadAndWrite(
            /* blob_size */ 100, /* part_size */ 100, /* max_response_size */ 30,
            ETagBehaviour::first_generation,
            ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = ETagBehaviour::second_generation, .honour_if_match = true});
        FAIL() << "Expected an exception on a source blob overwritten between two requests of one part";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::FILE_CHANGED_DURING_READ);
    }
}

/// A native copy of a source whose generation is the one the caller selected goes through, pinned
/// to that generation with a source-side `If-Match` in the quoted form the header wants.
TEST(AzureNativeCopy, UnchangedObject)
{
    for (bool asynchronous : {false, true})
    {
        NativeCopyOutcome outcome;
        ASSERT_NO_THROW(outcome = copyNatively(
            /* blob_size */ 100, ETagBehaviour::first_generation,
            ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true}, asynchronous));

        ASSERT_EQ(outcome.copied_generations, (std::vector<std::string>{ETagBehaviour::first_generation})) << "asynchronous = " << asynchronous;
        ASSERT_EQ(outcome.source_if_match_headers, (std::vector<std::string>{ETagBehaviour::first_generation})) << "asynchronous = " << asynchronous;
        ASSERT_TRUE(outcome.uploaded.empty()) << "asynchronous = " << asynchronous;
    }
}

/// The source blob is overwritten between the moment the caller looked at it (the listing or the
/// `HEAD` that produced its size and `ETag`) and the native copy. The endpoint evaluates the
/// source-side `If-Match`, so it refuses to copy the newer generation with `412 Precondition
/// Failed`, which must surface as the object having changed rather than as a silent copy of the
/// wrong generation, and must not be retried through the read-and-write fallback, which is pinned
/// to the same generation.
TEST(AzureNativeCopy, PreconditionFailedOnChangedSource)
{
    for (bool asynchronous : {false, true})
    {
        try
        {
            copyNatively(
                /* blob_size */ 100, ETagBehaviour::first_generation,
                ETagBehaviour{.etag = ETagBehaviour::second_generation, .etag_after_first = "", .honour_if_match = true}, asynchronous);
            FAIL() << "Expected an exception on a native copy of a source blob overwritten after it was selected (asynchronous = " << asynchronous << ")";
        }
        catch (const DB::Exception & e)
        {
            ASSERT_EQ(e.code(), DB::ErrorCodes::FILE_CHANGED_DURING_READ) << "asynchronous = " << asynchronous;
        }
    }
}

/// The tag the caller selected was read from a blob listing, whose `Etag` element carries the bare
/// tag: the source-side `If-Match` must still be sent in the quoted form, or a real endpoint would
/// refuse every pinned native copy of a listed blob.
TEST(AzureNativeCopy, BareExpectedETag)
{
    NativeCopyOutcome outcome;
    ASSERT_NO_THROW(outcome = copyNatively(
        /* blob_size */ 100, ETagBehaviour::first_generation_bare,
        ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true}, /* asynchronous */ false));

    ASSERT_EQ(outcome.copied_generations, (std::vector<std::string>{ETagBehaviour::first_generation}));
    ASSERT_EQ(outcome.source_if_match_headers, (std::vector<std::string>{ETagBehaviour::first_generation}));
}

/// A caller that does not know the generation of the source (an empty `src_etag`) gets an
/// unconditional native copy, with no source-side `If-Match` at all.
TEST(AzureNativeCopy, NoETagMeansNoCondition)
{
    NativeCopyOutcome outcome;
    ASSERT_NO_THROW(outcome = copyNatively(
        /* blob_size */ 100, /* expected_etag */ "",
        ETagBehaviour{.etag = ETagBehaviour::second_generation, .etag_after_first = "", .honour_if_match = true}, /* asynchronous */ false));

    ASSERT_EQ(outcome.copied_generations, (std::vector<std::string>{ETagBehaviour::second_generation}));
    ASSERT_EQ(outcome.source_if_match_headers, (std::vector<std::string>{""}));
}

/// A `StoredObject` that carries an `ETag` is deleted with `If-Match` in the quoted form the header
/// wants, and an object whose generation is the selected one is deleted.
TEST(AzureConditionalDelete, UnchangedObject)
{
    DeleteOutcome outcome;
    ASSERT_NO_THROW(outcome = deleteThroughObjectStorage(
        ETagBehaviour::first_generation,
        ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true}));

    ASSERT_EQ(outcome.deleted_generations, (std::vector<std::string>{ETagBehaviour::first_generation}));
    ASSERT_EQ(outcome.if_match_headers, (std::vector<std::string>{ETagBehaviour::first_generation}));
}

/// The object was overwritten after the caller selected its generation: the endpoint refuses the
/// pinned delete with `412 Precondition Failed`, which must surface as the object having changed
/// (not be swallowed as "the object does not exist" by the if-exists semantics), and the newer
/// generation must stay in place.
TEST(AzureConditionalDelete, PreconditionFailedOnChangedObject)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        100, 100, 100, /* send_etag */ true, /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = ETagBehaviour::second_generation, .etag_after_first = "", .honour_if_match = true});

    try
    {
        objectStorageOver(transport)->removeObjectIfExists(blobGeneration(ETagBehaviour::first_generation));
        FAIL() << "Expected an exception on a pinned delete of an object overwritten after it was selected";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::FILE_CHANGED_DURING_READ);
    }

    ASSERT_TRUE(transport->deletedGenerations().empty());
    ASSERT_EQ(transport->deleteIfMatchHeaders(), (std::vector<std::string>{ETagBehaviour::first_generation}));
}

/// The tag was read from a blob listing, whose `Etag` element carries the bare tag: `If-Match` must
/// still be sent quoted, or a real endpoint would refuse every pinned delete of a listed blob.
TEST(AzureConditionalDelete, BareETag)
{
    DeleteOutcome outcome;
    ASSERT_NO_THROW(outcome = deleteThroughObjectStorage(
        ETagBehaviour::first_generation_bare,
        ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true}));

    ASSERT_EQ(outcome.deleted_generations, (std::vector<std::string>{ETagBehaviour::first_generation}));
    ASSERT_EQ(outcome.if_match_headers, (std::vector<std::string>{ETagBehaviour::first_generation}));
}

/// A `StoredObject` without an `ETag` (every caller that deletes by path) gets the unconditional
/// delete it always got, with no `If-Match` at all.
TEST(AzureConditionalDelete, NoETagMeansNoCondition)
{
    DeleteOutcome outcome;
    ASSERT_NO_THROW(outcome = deleteThroughObjectStorage(
        /* expected_etag */ "",
        ETagBehaviour{.etag = ETagBehaviour::second_generation, .etag_after_first = "", .honour_if_match = true}));

    ASSERT_EQ(outcome.deleted_generations, (std::vector<std::string>{ETagBehaviour::second_generation}));
    ASSERT_EQ(outcome.if_match_headers, (std::vector<std::string>{""}));
}

/// A move of a source that nobody touches copies and deletes one and the same generation.
TEST(AzureMove, UnchangedSource)
{
    MoveOutcome outcome;
    ASSERT_NO_THROW(outcome = moveBlob(ETagBehaviour::first_generation, ETagBehaviour::first_generation, /* overwrite_between_copy_and_delete */ false));

    ASSERT_FALSE(outcome.copy_error_code.has_value());
    ASSERT_FALSE(outcome.delete_error_code.has_value());
    ASSERT_EQ(outcome.copied_generations, (std::vector<std::string>{ETagBehaviour::first_generation}));
    ASSERT_EQ(outcome.deleted_generations, (std::vector<std::string>{ETagBehaviour::first_generation}));
}

/// The source was overwritten after it was ingested and before the move started: the move is
/// pinned to the ingested generation, so the endpoint refuses the copy, nothing is copied, and the
/// newer generation, which was never ingested, is neither copied nor deleted.
TEST(AzureMove, SourceOverwrittenAfterIngestion)
{
    MoveOutcome outcome;
    ASSERT_NO_THROW(outcome = moveBlob(ETagBehaviour::first_generation, ETagBehaviour::second_generation, /* overwrite_between_copy_and_delete */ false));

    ASSERT_EQ(outcome.copy_error_code, std::optional<int>(DB::ErrorCodes::FILE_CHANGED_DURING_READ));
    ASSERT_TRUE(outcome.copied_generations.empty());
    ASSERT_TRUE(outcome.deleted_generations.empty());
}

/// The same-container `MOVE` copies through `AzureObjectStorage::copyObject`. A source that carries
/// the ingested generation and its size is copied without any further lookup: the read is pinned to
/// that generation and the whole object arrives at the destination.
TEST(AzureCopyObject, PinnedToCallerGeneration)
{
    CopyObjectOutcome outcome;
    ASSERT_NO_THROW(outcome = copyObjectThroughObjectStorage(ETagBehaviour::first_generation, /* caller_size */ 100, ETagBehaviour::first_generation));

    ASSERT_FALSE(outcome.error_code.has_value());
    ASSERT_EQ(outcome.copied.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(outcome.copied);
}

/// The source was overwritten after the caller selected its generation: the pinned read is refused
/// by the endpoint, the copy fails with the object having changed, and nothing reaches the
/// destination, so the newer generation is not copied under the name of the ingested one.
TEST(AzureCopyObject, CallerGenerationOverwritten)
{
    CopyObjectOutcome outcome;
    ASSERT_NO_THROW(outcome = copyObjectThroughObjectStorage(ETagBehaviour::first_generation, /* caller_size */ 100, ETagBehaviour::second_generation));

    ASSERT_EQ(outcome.error_code, std::optional<int>(DB::ErrorCodes::FILE_CHANGED_DURING_READ));
    ASSERT_TRUE(outcome.copied.empty());
}

/// The caller knows the generation but not its size: the `HEAD` that supplies the size must
/// describe that same generation, or the copy is refused before a single byte is read.
TEST(AzureCopyObject, SizeFromHeadOfAnotherGenerationIsRefused)
{
    CopyObjectOutcome outcome;
    ASSERT_NO_THROW(outcome = copyObjectThroughObjectStorage(ETagBehaviour::first_generation, DB::StoredObject::UnknownSize, ETagBehaviour::second_generation));

    ASSERT_EQ(outcome.error_code, std::optional<int>(DB::ErrorCodes::FILE_CHANGED_DURING_READ));
    ASSERT_TRUE(outcome.copied.empty());
}

/// The same lookup for an unchanged object supplies the size, and the copy proceeds pinned to
/// the caller's generation.
TEST(AzureCopyObject, SizeFromHeadOfSameGeneration)
{
    CopyObjectOutcome outcome;
    ASSERT_NO_THROW(outcome = copyObjectThroughObjectStorage(ETagBehaviour::first_generation, DB::StoredObject::UnknownSize, ETagBehaviour::first_generation));

    ASSERT_FALSE(outcome.error_code.has_value());
    ASSERT_EQ(outcome.copied.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(outcome.copied);
}

/// A source without a generation (every caller that copies by path) is copied the way it always
/// was: one `HEAD` supplies both the size and the generation the copy is then pinned to.
TEST(AzureCopyObject, UnpinnedSource)
{
    CopyObjectOutcome outcome;
    ASSERT_NO_THROW(outcome = copyObjectThroughObjectStorage(/* caller_etag */ "", DB::StoredObject::UnknownSize, ETagBehaviour::second_generation));

    ASSERT_FALSE(outcome.error_code.has_value());
    ASSERT_EQ(outcome.copied.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(outcome.copied);
}

/// The caller knows neither the generation nor the size, and the endpoint reports no `ETag` at
/// all: the `HEAD` that was made in order to pin the copy cannot name a generation, so the copy is
/// refused instead of transferring an unknown generation without `If-Match`.
TEST(AzureCopyObject, SourceWithoutETagFromTheHeadIsRefused)
{
    CopyObjectOutcome outcome;
    ASSERT_NO_THROW(outcome = copyObjectThroughObjectStorage(
        /* caller_etag */ "", DB::StoredObject::UnknownSize, ETagBehaviour::first_generation, /* send_etag */ false));

    ASSERT_EQ(outcome.error_code, std::optional<int>(DB::ErrorCodes::AZURE_BLOB_STORAGE_ERROR));
    ASSERT_TRUE(outcome.copied.empty());
}

/// The same refusal applies when only the size was missing: the generation the caller carries
/// cannot be checked against a `HEAD` that reports none, so the copy does not start.
TEST(AzureCopyObject, SizingHeadWithoutETagIsRefused)
{
    CopyObjectOutcome outcome;
    ASSERT_NO_THROW(outcome = copyObjectThroughObjectStorage(
        ETagBehaviour::first_generation, DB::StoredObject::UnknownSize, ETagBehaviour::first_generation, /* send_etag */ false));

    ASSERT_EQ(outcome.error_code, std::optional<int>(DB::ErrorCodes::AZURE_BLOB_STORAGE_ERROR));
    ASSERT_TRUE(outcome.copied.empty());
}

/// The source is overwritten after the copy has completed and before the delete: the delete is
/// pinned to the generation that was copied, so the endpoint refuses it, the move fails with the
/// object having changed, and the newer generation, which was never copied, is not deleted.
TEST(AzureMove, SourceOverwrittenBetweenCopyAndDelete)
{
    MoveOutcome outcome;
    ASSERT_NO_THROW(outcome = moveBlob(ETagBehaviour::first_generation, ETagBehaviour::first_generation, /* overwrite_between_copy_and_delete */ true));

    ASSERT_FALSE(outcome.copy_error_code.has_value());

    ASSERT_EQ(outcome.delete_error_code, std::optional<int>(DB::ErrorCodes::FILE_CHANGED_DURING_READ));
    ASSERT_EQ(outcome.copied_generations, (std::vector<std::string>{ETagBehaviour::first_generation}));
    ASSERT_TRUE(outcome.deleted_generations.empty());
}

/// The reader has already been handed bytes 0..99 by a single response when the caller consumes
/// 32 of them and then lowers the right bound to 40. The bytes 40..99 that the working buffer still
/// holds are past the new bound and must not be delivered: the read must end at byte 40 exactly.
TEST(AzureReadUntilPosition, BoundLoweredBelowBufferedBytes)
{
    auto buffer = makeFreshBuffer(/* max_response_size */ 100, /* blob_size */ 100);

    std::string head(32, '\0');
    ASSERT_EQ(buffer->read(head.data(), head.size()), static_cast<size_t>(32));
    assertCountsUpFromZero(head);

    buffer->setReadUntilPosition(40);

    std::string tail;
    ASSERT_NO_THROW(DB::readStringUntilEOF(tail, *buffer));
    ASSERT_EQ(tail.size(), static_cast<size_t>(8));
    for (size_t i = 0; i < tail.size(); ++i)
        ASSERT_EQ(static_cast<uint8_t>(tail[i]), static_cast<uint8_t>(32 + i)) << "at position " << 32 + i;
    ASSERT_EQ(buffer->getPosition(), 40);
}

/// The right bound is raised while the reader still holds bytes from the response of the previous
/// bound: the response answered the range 0..63, the caller consumes 32 bytes and asks to read
/// until byte 80. The read must continue from byte 32, reopening the download under the new bound,
/// and deliver exactly bytes 32..79.
TEST(AzureReadUntilPosition, BoundRaisedAfterBufferedBytes)
{
    auto buffer = makeFreshBuffer(/* max_response_size */ 100, /* blob_size */ 100);
    buffer->setReadUntilPosition(64);

    std::string head(32, '\0');
    ASSERT_EQ(buffer->read(head.data(), head.size()), static_cast<size_t>(32));
    assertCountsUpFromZero(head);

    buffer->setReadUntilPosition(80);

    std::string tail;
    ASSERT_NO_THROW(DB::readStringUntilEOF(tail, *buffer));
    ASSERT_EQ(tail.size(), static_cast<size_t>(48));
    for (size_t i = 0; i < tail.size(); ++i)
        ASSERT_EQ(static_cast<uint8_t>(tail[i]), static_cast<uint8_t>(32 + i)) << "at position " << 32 + i;
    ASSERT_EQ(buffer->getPosition(), 80);
}

/// Setting the same right bound again is not a new logical read: nothing already buffered is
/// dropped and the download is not reopened.
TEST(AzureReadUntilPosition, SameBoundSetTwice)
{
    auto buffer = makeFreshBuffer(/* max_response_size */ 100, /* blob_size */ 100);
    buffer->setReadUntilPosition(64);

    std::string head(32, '\0');
    ASSERT_EQ(buffer->read(head.data(), head.size()), static_cast<size_t>(32));
    ASSERT_EQ(buffer->available(), static_cast<size_t>(32));

    buffer->setReadUntilPosition(64);
    ASSERT_EQ(buffer->available(), static_cast<size_t>(32));

    std::string tail;
    ASSERT_NO_THROW(DB::readStringUntilEOF(tail, *buffer));
    ASSERT_EQ(tail.size(), static_cast<size_t>(32));
    ASSERT_EQ(buffer->getPosition(), 64);
}

/// A `StoredObject` of `bytes_size` 0 is a blob that the `LIST` or `HEAD` producing it reported as
/// empty. When the read is pinned to the generation that measurement describes, that size is as
/// trustworthy as any other locally known size, so the read set up by
/// `AzureObjectStorage::readObject` must end at once, no matter how many bytes a misbehaving
/// endpoint hands out for the object, both sequentially and through `readBigAt`.
TEST(AzureReadWithoutRightBound, KnownEmptyObject)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        /* max_response_size */ 100, /* served_size */ 100, /* blob_size */ 100, /* send_etag */ true,
        /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true});
    auto object_storage = objectStorageOver(transport);

    DB::StoredObject empty_object("blob", /* local_path */ "", /* bytes_size */ 0);
    empty_object.etag = ETagBehaviour::first_generation_bare;
    auto buffer = object_storage->readObject(empty_object, DB::ReadSettings{});

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));
    ASSERT_TRUE(data.empty());

    char byte = 0;
    ASSERT_EQ(buffer->readBigAt(&byte, 1, /* range_begin */ 0, /* progress_callback */ nullptr), static_cast<size_t>(0));
}

/// The same endpoint, read through a `StoredObject` whose size was never determined: only the
/// `StoredObject::UnknownSize` sentinel means that the size is unknown, and then the endpoint decides.
TEST(AzureReadWithoutRightBound, ObjectOfUnknownSize)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        /* max_response_size */ 100, /* served_size */ 100, /* blob_size */ 100, /* send_etag */ true);
    auto object_storage = objectStorageOver(transport);

    DB::StoredObject object("blob");
    ASSERT_EQ(object.bytes_size, DB::StoredObject::UnknownSize);
    auto buffer = object_storage->readObject(object, DB::ReadSettings{});

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));
    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// A size measured before the read - by the `LIST` or the `HEAD` that produced the `StoredObject` -
/// is local information, and it is the length of the file for every layer above this buffer, so it
/// is a hard end of the data whether or not the read is pinned to a generation. An endpoint that
/// answers with more data than that - because the blob was overwritten with a larger one, or because
/// it is simply not answering the request that was made - must not have the excess handed to the
/// caller under offsets that belong to the next object of a gather, to a cache entry, or to nothing
/// at all.
TEST(AzureKnownObjectSize, UnpinnedReadStopsAtTheKnownSize)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        /* max_response_size */ 200, /* served_size */ 200, /* blob_size */ 200, /* send_etag */ true);
    auto object_storage = objectStorageOver(transport);

    DB::StoredObject object("blob", /* local_path */ "", /* bytes_size */ 100);
    ASSERT_TRUE(object.etag.empty());
    auto buffer = object_storage->readObject(object, DB::ReadSettings{});

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));
    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The same for a positioned read: a range that begins past the known end of the object is the
/// documented end of file and is answered without a request at all.
TEST(AzureKnownObjectSize, UnpinnedPositionedReadPastTheKnownSize)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        /* max_response_size */ 200, /* served_size */ 200, /* blob_size */ 200, /* send_etag */ true);
    auto object_storage = objectStorageOver(transport);

    DB::StoredObject object("blob", /* local_path */ "", /* bytes_size */ 100);
    auto buffer = object_storage->readObject(object, DB::ReadSettings{});

    std::string data(50, '\0');
    ASSERT_EQ(buffer->readBigAt(data.data(), data.size(), /* range_begin */ 150, /* progress_callback */ nullptr), static_cast<size_t>(0));
}

/// A size of zero is a size like any other: an object that was listed as empty is read as empty,
/// sequentially and through `readBigAt`, whatever the endpoint is willing to serve for it. Only the
/// `StoredObject::UnknownSize` sentinel means that nothing is known - see `ObjectOfUnknownSize`.
TEST(AzureKnownObjectSize, ReadOfAnObjectListedAsEmpty)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        /* max_response_size */ 100, /* served_size */ 100, /* blob_size */ 100, /* send_etag */ true);
    auto object_storage = objectStorageOver(transport);

    DB::StoredObject object("blob", /* local_path */ "", /* bytes_size */ 0);
    ASSERT_TRUE(object.etag.empty());

    auto buffer = object_storage->readObject(object, DB::ReadSettings{});
    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));
    ASSERT_TRUE(data.empty());

    auto positioned_buffer = object_storage->readObject(object, DB::ReadSettings{});
    char byte = 0;
    ASSERT_EQ(positioned_buffer->readBigAt(&byte, 1, /* range_begin */ 0, /* progress_callback */ nullptr), static_cast<size_t>(0));
}

/// A read that is pinned to the generation the size was measured on stops there as well, and the
/// `If-Match` of the requests does not get in the way of it.
TEST(AzureKnownObjectSize, PinnedReadStopsAtTheSizeOfItsGeneration)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        /* max_response_size */ 200, /* served_size */ 200, /* blob_size */ 200, /* send_etag */ true,
        /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true});
    auto object_storage = objectStorageOver(transport);

    DB::StoredObject object("blob", /* local_path */ "", /* bytes_size */ 100);
    object.etag = ETagBehaviour::first_generation_bare;
    auto buffer = object_storage->readObject(object, DB::ReadSettings{});

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));
    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

namespace
{
/// The connection of a backup kept at the shared endpoint `transport`. The endpoint is addressed with
/// a SAS token, so the client is built straight from the URL, with no account lookup and no attempt
/// to create the container.
DB::AzureBlobStorage::ConnectionParams backupConnectionOver(const std::shared_ptr<MisbehavingRangeTransport> & transport)
{
    DB::AzureBlobStorage::ConnectionParams params;
    params.endpoint.storage_account_url = "http://azure.invalid";
    params.endpoint.container_name = "container";
    params.endpoint.sas_auth = "sv=2020-08-04";
    params.endpoint.container_already_exists = true;
    params.client_options.Retry.MaxRetries = 0;
    params.client_options.Transport.Transport = transport;
    return params;
}

/// A reader of the backup `backup` kept at the shared endpoint `transport`.
std::unique_ptr<DB::BackupReaderAzureBlobStorage> backupReaderOver(const std::shared_ptr<MisbehavingRangeTransport> & transport)
{
    return std::make_unique<DB::BackupReaderAzureBlobStorage>(
        backupConnectionOver(transport), /* blob_path */ "backup", /* allow_azure_native_copy */ false,
        DB::ReadSettings{}, DB::WriteSettings{}, getContext().context);
}

/// A writer of the backup `backup` kept at the shared endpoint `transport`, with the native copy enabled.
std::unique_ptr<DB::BackupWriterAzureBlobStorage> backupWriterOver(const std::shared_ptr<MisbehavingRangeTransport> & transport)
{
    /// The copies of the writer are scheduled on the backups pool, which the server initializes on startup.
    DB::getBackupsIOThreadPool().initializeWithDefaultSettingsIfNotInitialized();

    return std::make_unique<DB::BackupWriterAzureBlobStorage>(
        backupConnectionOver(transport), /* blob_path */ "backup", /* allow_azure_native_copy */ true,
        DB::ReadSettings{}, DB::WriteSettings{}, getContext().context, /* attempt_to_create_container */ false);
}


}

/// A blob of the backup is overwritten between the `HEAD` that selects its generation and the
/// response that carries its bytes. A restore must not stitch the two generations together: the
/// read is pinned to the generation the `HEAD` saw.
TEST(AzureBackupReader, ReadPinnedToBlobGeneration)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        100, 100, 100, /* send_etag */ true, /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = ETagBehaviour::second_generation, .honour_if_match = false});
    auto reader = backupReaderOver(transport);

    auto buffer = reader->readFile("file");
    std::string data;
    try
    {
        DB::readStringUntilEOF(data, *buffer);
        FAIL() << "Expected an exception on a backup blob whose generation changed after it was selected";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::FILE_CHANGED_DURING_READ);
    }
}

/// The blob is 100 bytes long according to the `HEAD`, but the endpoint keeps handing out bytes past
/// that. The size the backup saw is the end of the file; the endpoint does not get to extend it.
TEST(AzureBackupReader, ReadBoundedBySizeOfHead)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        /* max_response_size */ 40, /* served_size */ 200, /* blob_size */ 100, /* send_etag */ true);
    auto reader = backupReaderOver(transport);

    auto buffer = reader->readFile("file");
    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));
    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// An unchanged backup blob is read whole through a reader pinned to it.
TEST(AzureBackupReader, ReadUnchangedBlob)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        /* max_response_size */ 40, /* served_size */ 100, /* blob_size */ 100, /* send_etag */ true, /* reported_length */ std::nullopt,
        /* ignore_range */ false, ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true});
    auto reader = backupReaderOver(transport);

    auto buffer = reader->readFile("file");
    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));
    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// A copy inside the backup (the writer deduplicating a file against one it already wrote) is
/// pinned to the generation of the source that the `HEAD` before the copy saw.
TEST(AzureBackupWriter, CopyInsideBackupPinnedToSourceGeneration)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        100, 100, 100, /* send_etag */ true, /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true});
    auto writer = backupWriterOver(transport);

    ASSERT_NO_THROW(writer->copyFile(/* destination */ "copy", /* source */ "file", /* size */ 100));

    ASSERT_EQ(transport->nativelyCopiedGenerations(), (std::vector<std::string>{ETagBehaviour::first_generation}));
    ASSERT_EQ(transport->sourceIfMatchHeaders(), (std::vector<std::string>{ETagBehaviour::first_generation}));
}

/// The source of a copy inside the backup is overwritten between the `HEAD` and the copy; the
/// endpoint evaluates the source-side `If-Match`, so the copy is refused rather than copying the
/// newer generation under the name of the older one.
TEST(AzureBackupWriter, CopyInsideBackupRefusedOnChangedSource)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        100, 100, 100, /* send_etag */ true, /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = ETagBehaviour::second_generation, .honour_if_match = true});
    auto writer = backupWriterOver(transport);

    try
    {
        writer->copyFile(/* destination */ "copy", /* source */ "file", /* size */ 100);
        FAIL() << "Expected an exception on a copy of a backup blob overwritten after it was selected";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::FILE_CHANGED_DURING_READ);
    }
    ASSERT_TRUE(transport->nativelyCopiedGenerations().empty());
}

/// The blob is not the size the backup metadata recorded for it: it is not the blob the backup
/// wrote, so no copy is attempted at all.
TEST(AzureBackupWriter, CopyOfBlobOfAnotherSizeIsRefused)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        100, 100, /* blob_size */ 100, /* send_etag */ true);
    auto writer = backupWriterOver(transport);

    try
    {
        writer->copyFile(/* destination */ "copy", /* source */ "file", /* size */ 99);
        FAIL() << "Expected an exception on a backup blob whose size differs from the one the backup recorded";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::BACKUP_DAMAGED);
    }
    ASSERT_TRUE(transport->nativelyCopiedGenerations().empty());
    ASSERT_TRUE(transport->uploadedData().empty());
}

/// The whole-object copy of a file from an Azure disk into the backup takes the size and the generation
/// of the source blob from one `HEAD`: a blob of the size the disk reported yields the generation the
/// copy is pinned to.
TEST(AzureBackupWriter, WholeCopySourceMeasuredByTheHeadThatPinsIt)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        100, 100, /* blob_size */ 100, /* send_etag */ true, /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true});
    auto object_storage = objectStorageOver(transport);

    ASSERT_EQ(DB::headSourceBlobOfWholeCopy(*object_storage, "blob", /* expected_size */ 100), ETagBehaviour::first_generation);
}

/// The source blob was replaced by one of another size after the disk reported the size of the file.
/// The `HEAD` sees the new generation with its new size, so the copy is refused instead of being pinned
/// to a generation that is not `length` bytes long.
TEST(AzureBackupWriter, WholeCopyOfSourceReplacedByAnotherSizeIsRefused)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        150, 150, /* blob_size */ 150, /* send_etag */ true, /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = ETagBehaviour::second_generation, .etag_after_first = "", .honour_if_match = true});
    auto object_storage = objectStorageOver(transport);

    try
    {
        DB::headSourceBlobOfWholeCopy(*object_storage, "blob", /* expected_size */ 100);
        FAIL() << "Expected an exception on a source blob whose size differs from the one the disk reported";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::FILE_CHANGED_DURING_READ);
    }
    ASSERT_TRUE(transport->nativelyCopiedGenerations().empty());
    ASSERT_TRUE(transport->uploadedData().empty());
}

/// The endpoint reports no `ETag` for the blobs of the backup: a read cannot be pinned to one
/// generation, so a restore would take whatever generation the `GET` happens to meet for the one
/// the `HEAD` measured. The read is refused instead, before any byte is asked for.
TEST(AzureBackupReader, EndpointWithoutETagIsRefused)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(100, 100, 100, /* send_etag */ false);
    auto reader = backupReaderOver(transport);

    try
    {
        reader->readFile("file");
        FAIL() << "Expected an exception on a backup blob whose generation the endpoint does not report";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::AZURE_BLOB_STORAGE_ERROR);
    }
    ASSERT_EQ(transport->headRequests(), 1u);
}

/// The same for a copy inside the backup: without a generation to pin the copy to, a same-size
/// overwrite between the `HEAD` and the copy would be copied under the name of the blob the backup
/// wrote, so no copy is made.
TEST(AzureBackupWriter, CopyInsideBackupRefusedWithoutETag)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(100, 100, 100, /* send_etag */ false);
    auto writer = backupWriterOver(transport);

    try
    {
        writer->copyFile(/* destination */ "copy", /* source */ "file", /* size */ 100);
        FAIL() << "Expected an exception on a copy of a backup blob whose generation the endpoint does not report";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::AZURE_BLOB_STORAGE_ERROR);
    }
    ASSERT_TRUE(transport->nativelyCopiedGenerations().empty());
    ASSERT_TRUE(transport->uploadedData().empty());
}

/// And for the whole-object copy of a file from an Azure disk into the backup: a source blob of the
/// right size but of no reported generation is not copied either.
TEST(AzureBackupWriter, WholeCopyRefusedWithoutETag)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(100, 100, 100, /* send_etag */ false);
    auto object_storage = objectStorageOver(transport);

    try
    {
        DB::headSourceBlobOfWholeCopy(*object_storage, "blob", /* expected_size */ 100);
        FAIL() << "Expected an exception on a source blob whose generation the endpoint does not report";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::AZURE_BLOB_STORAGE_ERROR);
    }
    ASSERT_EQ(transport->headRequests(), 1u);
}

/// The entry of a blob listing for the blob `blob` of 100 bytes, whose `Etag` element is `etag`
/// (an endpoint that omits the element yields an empty one).
static DB::RelativePathWithMetadata listingEntry(const std::string & etag)
{
    DB::ObjectMetadata metadata;
    metadata.size_bytes = 100;
    metadata.etag = etag;
    return DB::RelativePathWithMetadata("blob", metadata);
}

/// An Azure `MOVE` copies and deletes the generation that was ingested, and an Azure `DELETE`
/// deletes it: both act on that generation, so for both it must be known up front. A `TAG` or a
/// `KEEP` leaves the object as it is, and the S3 post-processing is not pinned to a generation.
TEST(AzureIngestedGeneration, AzureMoveAndDeleteNeedIt)
{
    ASSERT_TRUE(DB::afterProcessingNeedsIngestedGeneration(DB::ObjectStorageType::Azure, DB::ObjectStorageQueueAction::MOVE));
    ASSERT_TRUE(DB::afterProcessingNeedsIngestedGeneration(DB::ObjectStorageType::Azure, DB::ObjectStorageQueueAction::DELETE));
    ASSERT_FALSE(DB::afterProcessingNeedsIngestedGeneration(DB::ObjectStorageType::Azure, DB::ObjectStorageQueueAction::KEEP));
    ASSERT_FALSE(DB::afterProcessingNeedsIngestedGeneration(DB::ObjectStorageType::Azure, DB::ObjectStorageQueueAction::TAG));
    ASSERT_FALSE(DB::afterProcessingNeedsIngestedGeneration(DB::ObjectStorageType::S3, DB::ObjectStorageQueueAction::MOVE));
    ASSERT_FALSE(DB::afterProcessingNeedsIngestedGeneration(DB::ObjectStorageType::S3, DB::ObjectStorageQueueAction::DELETE));
}

/// The listing reported the generation: it is used as is, and the endpoint is not asked again.
TEST(AzureIngestedGeneration, ListingWithETagNeedsNoHead)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(100, 100, 100, /* send_etag */ true);
    auto object_storage = objectStorageOver(transport);

    auto entry = listingEntry(ETagBehaviour::first_generation_bare);
    ASSERT_TRUE(DB::useIngestedGenerationOfTheListedObject(entry));
    ASSERT_TRUE(entry.require_read_pinned_to_generation);
    ASSERT_EQ(entry.metadata->etag, ETagBehaviour::first_generation_bare);
    ASSERT_EQ(entry.metadata->size_bytes, 100u);
    ASSERT_EQ(transport->headRequests(), 0u);
}

/// `ListBlobs` omitted the `Etag` element. The generation is not recovered with a `HEAD`: that
/// `HEAD` would run after the path was listed and claimed in Keeper, so it could name a generation
/// `B` that replaced the listed generation `A` in the meantime, and the queue - which keys its
/// state by path - would ingest, move or delete `B` and mark the path processed, skipping `A`
/// forever. The generation stays unknown, which the source turns into a failed file, and nothing
/// is asked of the endpoint at all.
TEST(AzureIngestedGeneration, ListingWithoutETagIsNotRecoveredByAHead)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        100, 100, 100, /* send_etag */ true, /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true});
    auto object_storage = objectStorageOver(transport);

    auto entry = listingEntry("");
    ASSERT_FALSE(DB::useIngestedGenerationOfTheListedObject(entry));
    ASSERT_FALSE(entry.require_read_pinned_to_generation);
    ASSERT_TRUE(entry.metadata->etag.empty());
    ASSERT_EQ(transport->headRequests(), 0u);
}

/// The generation the listing reported is served by the read pinned to it (the way
/// `StorageObjectStorageSource` pins every read to the generation of its `ObjectInfo`).
TEST(AzureIngestedGeneration, ListedGenerationIsTheOneRead)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        100, 100, 100, /* send_etag */ true, /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true});
    auto object_storage = objectStorageOver(transport);

    auto entry = listingEntry(ETagBehaviour::first_generation_bare);
    ASSERT_TRUE(DB::useIngestedGenerationOfTheListedObject(entry));

    DB::StoredObject object("blob", /* local_path */ "", entry.metadata->size_bytes);
    object.etag = entry.metadata->etag;
    auto buffer = object_storage->readObject(object, DB::ReadSettings{});

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));
    ASSERT_EQ(data.size(), 100u);
    assertCountsUpFromZero(data);
}

/// The blob is overwritten between the listing that reported the generation and the read: the read
/// pinned to that generation is refused by the endpoint, so the generation the post-processing
/// would later act on is never one that was not ingested.
TEST(AzureIngestedGeneration, OverwrittenBetweenListingAndRead)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        100, 100, 100, /* send_etag */ true, /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true});
    auto object_storage = objectStorageOver(transport);

    auto entry = listingEntry(ETagBehaviour::first_generation_bare);
    ASSERT_TRUE(DB::useIngestedGenerationOfTheListedObject(entry));

    transport->overwriteObject(ETagBehaviour::second_generation);

    DB::StoredObject object("blob", /* local_path */ "", entry.metadata->size_bytes);
    object.etag = entry.metadata->etag;
    auto buffer = object_storage->readObject(object, DB::ReadSettings{});

    std::string data;
    try
    {
        DB::readStringUntilEOF(data, *buffer);
        FAIL() << "the read of an overwritten generation must fail";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::FILE_CHANGED_DURING_READ);
    }
}

/// The endpoint reports no `ETag` in the listing: the generation stays unknown, which the queue
/// source turns into a failed file rather than a read.
TEST(AzureIngestedGeneration, EndpointWithoutETag)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(100, 100, 100, /* send_etag */ false);
    auto object_storage = objectStorageOver(transport);

    auto entry = listingEntry("");
    ASSERT_FALSE(DB::useIngestedGenerationOfTheListedObject(entry));
    ASSERT_FALSE(entry.require_read_pinned_to_generation);
    ASSERT_TRUE(entry.metadata->etag.empty());
    ASSERT_EQ(entry.metadata->size_bytes, 100u);
    ASSERT_EQ(transport->headRequests(), 0u);
}

namespace
{

/// The metadata of an `ObjectStorageQueue` table whose `after_processing` is `action`.
DB::ObjectStorageQueueTableMetadata queueTableMetadataWith(const std::string & action)
{
    return DB::ObjectStorageQueueTableMetadata::parse(
        "{\"format_name\": \"CSV\", \"columns\": \"columns format version: 1\\n0 columns:\\n\", "
        "\"mode\": \"unordered\", \"after_processing\": \"" + action + "\"}");
}

/// What the `after_processing` step of an Azure `ObjectStorageQueue` table (the step that runs just
/// before the batch is committed to Keeper) did to the endpoint.
struct PostProcessingOutcome
{
    /// The generation that each successful `Delete Blob` removed.
    std::vector<std::string> deleted_generations;
    /// The bytes uploaded to the destination of a move.
    std::string copied;
    /// The error code `process` threw, if it did throw.
    std::optional<int> error_code;
};

/// Runs the `after_processing = action` step of an Azure table over `objects`, the way a commit does,
/// against an endpoint that holds `generation_at_endpoint` and evaluates `If-Match`. A `MOVE` moves
/// within the container, under the prefix `moved`.
PostProcessingOutcome postProcess(const std::string & action, const DB::StoredObjects & objects, const std::string & generation_at_endpoint)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        100, 100, 100, /* send_etag */ true, /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = generation_at_endpoint, .etag_after_first = "", .honour_if_match = true});
    std::shared_ptr<DB::IObjectStorage> object_storage = objectStorageOver(transport);

    const auto table_metadata = queueTableMetadataWith(action);
    DB::ObjectStorageQueuePostProcessor::AfterProcessingSettings settings;
    settings.after_processing_retries = 1;
    settings.after_processing_move_prefix = "moved";

    DB::ObjectStorageQueuePostProcessor post_processor(
        getContext().context, DB::ObjectStorageType::Azure, object_storage, "AzureQueue", table_metadata, settings);

    std::optional<int> error_code;
    DB::UnorderedSetWithMemoryTracking<String> failed_object_paths;
    try
    {
        post_processor.process(objects, failed_object_paths);
    }
    catch (const DB::Exception & e)
    {
        error_code = e.code();
    }

    return PostProcessingOutcome{
        .deleted_generations = transport->deletedGenerations(), .copied = transport->uploadedData(), .error_code = error_code};
}

/// A `StoredObject` for the blob `blob` of 100 bytes, as `prepareCommitRequests` hands it over: the
/// generation that was ingested is `etag`.
DB::StoredObject ingestedGeneration(const std::string & etag)
{
    DB::StoredObject object("blob", /* local_path */ "", 100);
    object.etag = etag;
    return object;
}

}

/// The source of a `MOVE` is still the generation that was ingested: it is copied and deleted, and
/// the step completes, so the batch is committed.
TEST(AzurePostProcessing, MoveOfIngestedGenerationCompletes)
{
    const auto outcome = postProcess("move", {ingestedGeneration(ETagBehaviour::first_generation)}, ETagBehaviour::first_generation);

    ASSERT_FALSE(outcome.error_code.has_value());
    ASSERT_EQ(outcome.copied.size(), 100u);
    ASSERT_EQ(outcome.deleted_generations, (std::vector<std::string>{ETagBehaviour::first_generation}));
}

/// The source of a `MOVE` was overwritten after it was ingested. Reporting that to the log only would
/// let the commit mark the file processed while the newer generation, never ingested, stays in the
/// bucket untracked. The step throws `FILE_CHANGED_DURING_READ` out of `process` instead, so the
/// commit does not happen and the file is ingested again as its newer generation; the object itself
/// is left in place.
TEST(AzurePostProcessing, MoveOfOverwrittenGenerationAbortsTheCommit)
{
    const auto outcome = postProcess("move", {ingestedGeneration(ETagBehaviour::first_generation)}, ETagBehaviour::second_generation);

    ASSERT_EQ(outcome.error_code, DB::ErrorCodes::FILE_CHANGED_DURING_READ);
    ASSERT_TRUE(outcome.copied.empty());
    ASSERT_TRUE(outcome.deleted_generations.empty());
}

/// An Azure `DELETE` acts on the ingested generation as well, so the source never hands over an
/// object whose generation is unknown; if one did arrive, deleting it by path could delete a
/// generation that was never ingested. The step refuses it, and nothing is deleted.
TEST(AzurePostProcessing, DeleteOfUntaggedObjectIsRefused)
{
    const auto outcome = postProcess("delete", {ingestedGeneration("")}, ETagBehaviour::first_generation);

    ASSERT_EQ(outcome.error_code, DB::ErrorCodes::AZURE_BLOB_STORAGE_ERROR);
    ASSERT_TRUE(outcome.deleted_generations.empty());
}

/// The read that `ObjectStorageQueue` opens goes through `createReadBuffer`, which pins a plain read
/// to the generation of its `ObjectInfo` only while `s3_validate_etag_on_read` is on. That setting
/// is about torn reads and can be turned off, directly or through `compatibility`, so a queue whose
/// post-processing acts on the ingested generation cannot depend on it: the generation the read
/// serves has to be the generation the move or the delete acts on afterwards. These two tests read
/// through `createReadBuffer` with the setting off, against an endpoint that replaces the blob
/// between the metadata probe and the `GET`.

/// With the requirement recorded by `useIngestedGenerationOfTheListedObject`, the read is pinned even though the
/// setting is off: the newer generation is refused instead of being ingested behind the move.
TEST(AzureQueueReadPinning, PinnedWithValidateETagOnReadDisabled)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        100, 100, 100, /* send_etag */ true, /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = ETagBehaviour::second_generation, .honour_if_match = true});
    DB::ObjectStoragePtr object_storage = objectStorageOver(transport);

    auto context = DB::Context::createCopy(getContext().context);
    context->setSetting("s3_validate_etag_on_read", false);

    auto entry = listingEntry(ETagBehaviour::first_generation_bare);
    ASSERT_TRUE(DB::useIngestedGenerationOfTheListedObject(entry));
    ASSERT_TRUE(entry.require_read_pinned_to_generation);

    transport->overwriteObject(ETagBehaviour::second_generation);

    auto buffer = DB::createReadBuffer(entry, object_storage, context, getLogger("AzureQueueReadPinning"));

    std::string data;
    try
    {
        DB::readStringUntilEOF(data, *buffer);
        FAIL() << "the read of a generation other than the ingested one must fail";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::FILE_CHANGED_DURING_READ);
    }
}

/// The same read without that requirement: with the setting off nothing pins it, and the endpoint
/// serves whatever generation it holds. This is what a plain read of the table does, and what the
/// queue must not do.
TEST(AzureQueueReadPinning, PlainReadIsNotPinnedWhenTheSettingIsDisabled)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        100, 100, 100, /* send_etag */ true, /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = ETagBehaviour::second_generation, .honour_if_match = true});
    DB::ObjectStoragePtr object_storage = objectStorageOver(transport);

    auto context = DB::Context::createCopy(getContext().context);
    context->setSetting("s3_validate_etag_on_read", false);

    auto entry = listingEntry(ETagBehaviour::first_generation_bare);
    ASSERT_TRUE(DB::useIngestedGenerationOfTheListedObject(entry));
    entry.require_read_pinned_to_generation = false;

    transport->overwriteObject(ETagBehaviour::second_generation);

    auto buffer = DB::createReadBuffer(entry, object_storage, context, getLogger("AzureQueueReadPinning"));

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));
    ASSERT_EQ(data.size(), 100u);
}

/// The size seen before the read does not only reach `ReadBufferFromAzureBlobStorage`:
/// `createReadBuffer` also exports it to the outer layers of the read as `StoredObject::bytes_size`,
/// where the bounded fast paths take it as the end of the data - `ReaderExecutor` clamps its object
/// reads to it, `AsynchronousBoundedReadBuffer` takes it as its right bound, and the filesystem
/// cache takes it as the file size. The buffer and those layers must agree on where the file ends,
/// whether or not the read is pinned to a generation, so all of these tests list the blob as 100
/// bytes while the endpoint holds 200 and expect exactly the listed 100 bytes.

/// The `ReaderExecutor` path (`use_reader_executor`).
TEST(AzureListedSizeThroughCreateReadBuffer, ReaderExecutorBoundsAnUnpinnedRead)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        /* max_response_size */ 200, /* served_size */ 200, /* blob_size */ 200, /* send_etag */ true);
    DB::ObjectStoragePtr object_storage = objectStorageOver(transport);

    auto context = DB::Context::createCopy(getContext().context);
    context->setSetting("s3_validate_etag_on_read", false);

    auto read_settings = context->getReadSettings();
    read_settings.reader_executor.enabled = true;
    read_settings.reader_executor.use_long_connections = false;
    /// The executor implements no async prefetch, so a pipeline that prefetches falls back to the
    /// legacy path: this read has to be the one the executor serves.
    read_settings.remote_fs_settings.prefetch = false;

    auto entry = listingEntry(ETagBehaviour::first_generation_bare);
    auto buffer = DB::createReadBuffer(
        entry, object_storage, context, getLogger("AzureListedSizeThroughCreateReadBuffer"), read_settings);

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));
    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The async prefetch path (`use_prefetch` / `needAsyncPrefetch`). `AsynchronousBoundedReadBuffer`
/// takes the size of the buffer it wraps in its constructor and `setReadUntilEnd` makes it the hard
/// right bound of the read; the buffer underneath reports the listed size as that size, so the two
/// bound the read at the same offset and the small-object fast path is kept for an unpinned read.
TEST(AzureListedSizeThroughCreateReadBuffer, AsyncPrefetchBoundsAnUnpinnedRead)
{
    /// The endpoint reports the 100 bytes of the listing when the prefetching wrapper asks for the
    /// size of the file, and then serves 200 bytes for an open-ended request.
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        /* max_response_size */ 200, /* served_size */ 200, /* blob_size */ 100, /* send_etag */ true);
    DB::ObjectStoragePtr object_storage = objectStorageOver(transport);

    auto context = DB::Context::createCopy(getContext().context);
    context->setSetting("s3_validate_etag_on_read", false);

    auto read_settings = context->getReadSettings();
    read_settings.remote_fs_settings.method = DB::RemoteFSReadMethod::threadpool;
    read_settings.remote_fs_settings.prefetch = true;

    auto entry = listingEntry(ETagBehaviour::first_generation_bare);
    auto buffer = DB::createReadBuffer(
        entry, object_storage, context, getLogger("AzureListedSizeThroughCreateReadBuffer"), read_settings);
    ASSERT_NE(typeid_cast<DB::AsynchronousBoundedReadBuffer *>(buffer.get()), nullptr);

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));
    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The same on the pinned read, where the `If-Match` of every request is in play as well.
TEST(AzureListedSizeThroughCreateReadBuffer, AsyncPrefetchBoundsAPinnedRead)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        /* max_response_size */ 200, /* served_size */ 200, /* blob_size */ 100, /* send_etag */ true,
        /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true});
    DB::ObjectStoragePtr object_storage = objectStorageOver(transport);

    auto context = DB::Context::createCopy(getContext().context);

    auto read_settings = context->getReadSettings();
    read_settings.remote_fs_settings.method = DB::RemoteFSReadMethod::threadpool;
    read_settings.remote_fs_settings.prefetch = true;

    auto entry = listingEntry(ETagBehaviour::first_generation_bare);
    auto buffer = DB::createReadBuffer(
        entry, object_storage, context, getLogger("AzureListedSizeThroughCreateReadBuffer"), read_settings);
    ASSERT_NE(typeid_cast<DB::AsynchronousBoundedReadBuffer *>(buffer.get()), nullptr);

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));
    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// The filesystem cache path (`filesystem_cache_name` with `enable_filesystem_cache`). The listed
/// `ETag` is a usable cache key here, so the object is cached - and served - as the 100-byte file
/// the listing described.
TEST(AzureListedSizeThroughCreateReadBuffer, FilesystemCacheBoundsAnUnpinnedRead)
{
    const std::string cache_name = "azure_listed_size_through_create_read_buffer";
    const auto cache_path = std::filesystem::current_path() / (cache_name + "_cache");
    std::filesystem::remove_all(cache_path);
    std::filesystem::create_directories(cache_path);
    SCOPE_EXIT({
        DB::FileCacheFactory::instance().clear();
        std::filesystem::remove_all(cache_path);
    });

    /// The cache derives the id of its common origin from the server UUID.
    DB::ServerUUID::setRandomForUnitTests();

    DB::FileCacheSettings cache_settings;
    cache_settings[DB::FileCacheSetting::path] = cache_path.string() + "/";
    cache_settings[DB::FileCacheSetting::max_size] = 1024 * 1024;
    cache_settings[DB::FileCacheSetting::max_elements] = 100;
    cache_settings[DB::FileCacheSetting::boundary_alignment] = 0;
    cache_settings[DB::FileCacheSetting::max_file_segment_size] = 1024 * 1024;
    cache_settings[DB::FileCacheSetting::background_download_threads] = 0;
    DB::FileCacheFactory::instance().getOrCreate(cache_name, cache_settings, "")->initialize();

    auto transport = std::make_shared<MisbehavingRangeTransport>(
        /* max_response_size */ 200, /* served_size */ 200, /* blob_size */ 200, /* send_etag */ true);
    DB::ObjectStoragePtr object_storage = objectStorageOver(transport);

    auto context = DB::Context::createCopy(getContext().context);
    context->setSetting("s3_validate_etag_on_read", false);
    context->setSetting("filesystem_cache_name", cache_name);

    auto read_settings = context->getReadSettings();
    read_settings.enable_filesystem_cache = true;

    auto entry = listingEntry(ETagBehaviour::first_generation_bare);
    auto buffer = DB::createReadBuffer(
        entry, object_storage, context, getLogger("AzureListedSizeThroughCreateReadBuffer"), read_settings);

    std::string data;
    ASSERT_NO_THROW(DB::readStringUntilEOF(data, *buffer));
    ASSERT_EQ(data.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(data);
}

/// A move of a file of a `plain_rewritable` disk copies the blob of the file and then deletes it.
/// Nothing happens to the blob in between here, so the move goes through and the generation that
/// was copied is the generation that was deleted.
TEST(AzurePlainRewritableMove, DeletesTheGenerationItCopied)
{
    const auto outcome = movePlainRewritableFile(/* overwrite_between_copy_and_delete */ false);

    ASSERT_FALSE(outcome.error_code.has_value());
    ASSERT_EQ(outcome.copied.size(), static_cast<size_t>(100));
    assertCountsUpFromZero(outcome.copied);
    ASSERT_EQ(outcome.deleted_generations, std::vector<std::string>{ETagBehaviour::first_generation});
}

/// Somebody overwrites the file between the copy and the delete. The delete is pinned to the
/// generation that was copied, so it removes nothing: the new generation of the file stays where it
/// is, and the move is reported as failed instead of taking away content nobody has moved.
TEST(AzurePlainRewritableMove, DoesNotDeleteAGenerationItDidNotCopy)
{
    const auto outcome = movePlainRewritableFile(/* overwrite_between_copy_and_delete */ true);

    ASSERT_TRUE(outcome.error_code.has_value());
    ASSERT_EQ(*outcome.error_code, DB::ErrorCodes::FILE_CHANGED_DURING_READ);
    ASSERT_EQ(outcome.copied.size(), static_cast<size_t>(100));
    ASSERT_TRUE(outcome.deleted_generations.empty());
}

/// The same overwrite when the two requests address the blob by path alone, as they did before the
/// move was pinned: the delete succeeds and takes away the generation that was never copied. This
/// is the data loss the pinning prevents, and it keeps the test above from being vacuous.
TEST(AzurePlainRewritableMove, AnUnpinnedMoveDeletesTheNewGeneration)
{
    const auto outcome = movePlainRewritableFile(/* overwrite_between_copy_and_delete */ true, /* pin */ false);

    ASSERT_FALSE(outcome.error_code.has_value());
    ASSERT_EQ(outcome.copied.size(), static_cast<size_t>(100));
    ASSERT_EQ(outcome.deleted_generations, std::vector<std::string>{ETagBehaviour::second_generation});
}

/// An endpoint that reports no `ETag` for the blob leaves the move nothing to pin to. It is refused
/// before anything is copied or deleted, rather than performed blind.
TEST(AzurePlainRewritableMove, ASourceWithoutAnETagIsRefused)
{
    const auto outcome
        = movePlainRewritableFile(/* overwrite_between_copy_and_delete */ false, /* pin */ true, /* send_etag */ false);

    ASSERT_TRUE(outcome.error_code.has_value());
    ASSERT_EQ(*outcome.error_code, DB::ErrorCodes::AZURE_BLOB_STORAGE_ERROR);
    ASSERT_TRUE(outcome.copied.empty());
    ASSERT_TRUE(outcome.deleted_generations.empty());
}

/// Rolling back a `plain_rewritable` operation after its remote delete succeeded. The key is free
/// by then, so another writer can recreate it, and the blob it puts there is a generation this
/// transaction has never seen. The rollback asks before it writes: a key that something is at may
/// not be restored over.
TEST(AzurePlainRewritableRollback, ARecreatedKeyIsNotRestoredOver)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        100, 100, 100, /* send_etag */ true, /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = ETagBehaviour::second_generation, .etag_after_first = "", .honour_if_match = true});
    auto object_storage = objectStorageOver(transport);

    ASSERT_FALSE(DB::aRollbackMayWriteOver(*object_storage, "blob"));
}

/// The same rollback when the key really is free: nobody recreated the blob this transaction
/// deleted, so the copy it saved aside is put back. This keeps the test above from passing for the
/// wrong reason - by refusing every restore.
TEST(AzurePlainRewritableRollback, AFreeKeyIsRestored)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        100, 100, 100, /* send_etag */ true, /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true},
        /* blob_size_after_first */ std::nullopt, /* refuse_range_past_the_data */ false, /* blob_missing */ true);
    auto object_storage = objectStorageOver(transport);

    ASSERT_TRUE(DB::aRollbackMayWriteOver(*object_storage, "blob"));
}

/// A move whose copy to the destination succeeded is rolled back: the blob the copy wrote has to be
/// taken back out, because the directory of a `plain_rewritable` metadata storage is rebuilt from
/// the blobs that are in the bucket and the move was never committed. The delete is pinned to the
/// generation the copy wrote, so a generation that another writer has put at the same key since is
/// refused and stays where it is.
TEST(AzurePlainRewritableRollback, TheDestinationDeleteIsPinnedToWhatTheCopyWrote)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        100, 100, 100, /* send_etag */ true, /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true});
    auto object_storage = objectStorageOver(transport);

    const DB::StoredObject destination = DB::nameTheGenerationThatWasJustWritten(*object_storage, "blob");
    ASSERT_EQ(destination.etag, ETagBehaviour::first_generation);

    transport->overwriteObject(ETagBehaviour::second_generation);

    std::optional<int> error_code;
    try
    {
        object_storage->removeObjectIfExists(destination);
    }
    catch (const DB::Exception & e)
    {
        error_code = e.code();
    }

    ASSERT_TRUE(error_code.has_value());
    ASSERT_EQ(*error_code, DB::ErrorCodes::FILE_CHANGED_DURING_READ);
    ASSERT_TRUE(transport->deletedGenerations().empty());
}

/// The same rollback of a delete that addresses the destination by path alone, as it did before it
/// was pinned: it takes away the generation another writer has just put there. That is the loss the
/// pinning prevents, and it keeps the test above honest.
TEST(AzurePlainRewritableRollback, AnUnpinnedDestinationDeleteTakesAwayTheNewGeneration)
{
    auto transport = std::make_shared<MisbehavingRangeTransport>(
        100, 100, 100, /* send_etag */ true, /* reported_length */ std::nullopt, /* ignore_range */ false,
        ETagBehaviour{.etag = ETagBehaviour::first_generation, .etag_after_first = "", .honour_if_match = true});
    auto object_storage = objectStorageOver(transport);

    transport->overwriteObject(ETagBehaviour::second_generation);

    ASSERT_NO_THROW(object_storage->removeObjectIfExists(DB::StoredObject("blob")));
    ASSERT_EQ(transport->deletedGenerations(), std::vector<std::string>{ETagBehaviour::second_generation});
}

/// The blob of a file the metadata says exists is not there when the generation is named. Returning
/// an unpinned object here would not stop the move: `copyObject` makes a `HEAD` of its own for a
/// source without an `ETag` and would pin the copy to a blob recreated in between, while the delete
/// that follows would still address the blob by path alone and take away whatever is there by then.
/// The move is refused before either request is made.
TEST(AzurePlainRewritableMove, AMissingSourceIsRefused)
{
    const auto outcome = movePlainRewritableFile(
        /* overwrite_between_copy_and_delete */ false, /* pin */ true, /* send_etag */ true, /* blob_missing */ true);

    ASSERT_TRUE(outcome.error_code.has_value());
    ASSERT_EQ(*outcome.error_code, DB::ErrorCodes::FILE_DOESNT_EXIST);
    ASSERT_TRUE(outcome.copied.empty());
    ASSERT_TRUE(outcome.deleted_generations.empty());
}

#endif
