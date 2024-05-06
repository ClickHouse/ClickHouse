#include <Processors/Transforms/NumberBlocksTransform.h>

#include <IO/WriteHelpers.h>

#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>


#include <fmt/core.h>


namespace DB
{
namespace DeduplicationToken
{

String DB::DeduplicationToken::TokenInfo::getToken(bool enable_assert) const
{
    chassert(stage == MATERIALIZE_VIEW_ID || !enable_assert);

    String result;
    result.reserve(getTotalSize());

    for (const auto & part : parts)
        result.append(part);

    return result;
}

void DB::DeduplicationToken::TokenInfo::setInitialToken(String part)
{
    chassert(stage == INITIAL);
    addTokenPart(std::move(part));
    stage = MATERIALIZE_VIEW_ID;
}

void TokenInfo::setUserToken(const String & token)
{
    chassert(stage == INITIAL);
    addTokenPart(fmt::format("user-token-{}", token));
    stage = SOURCE_BLOCK_NUMBER;
}

void TokenInfo::setSourceBlockNumber(size_t sbn)
{
    chassert(stage == SOURCE_BLOCK_NUMBER);
    addTokenPart(fmt::format(":source-number-{}", sbn));
    stage = MATERIALIZE_VIEW_ID;
}

void TokenInfo::setMaterializeViewID(const String & id)
{
    chassert(stage == MATERIALIZE_VIEW_ID);
    addTokenPart(fmt::format(":mv-{}", id));
    stage = MATERIALIZE_VIEW_BLOCK_NUMBER;
}

void TokenInfo::setMaterializeViewBlockNumber(size_t mvbn)
{
    chassert(stage == MATERIALIZE_VIEW_BLOCK_NUMBER);
    addTokenPart(fmt::format(":mv-bn-{}", mvbn));
    stage = MATERIALIZE_VIEW_ID;
}

void TokenInfo::reset()
{
    stage = INITIAL;
    parts.clear();
}

void TokenInfo::addTokenPart(String part)
{
    if (!part.empty())
        parts.push_back(std::move(part));
}

size_t TokenInfo::getTotalSize() const
{
    size_t size = 0;
    for (const auto & part : parts)
        size += part.size();
    return size;
}

void CheckTokenTransform::transform(Chunk & chunk)
{
    auto token_info = chunk.getChunkInfos().get<TokenInfo>();

    if (!token_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk has to have DedupTokenInfo as ChunkInfo, {}", debug);

    if (!must_be_present)
    {
        LOG_DEBUG(getLogger("CheckInsertDeduplicationTokenTransform"), "{}, no token required, token {}", debug, token_info->getToken(false));
        return;
    }

    LOG_DEBUG(getLogger("CheckInsertDeduplicationTokenTransform"), "{}, token: {}", debug, token_info->getToken(false));
}

void SetInitialTokenTransform::transform(Chunk & chunk)
{
    auto token_builder = chunk.getChunkInfos().get<TokenInfo>();
    chassert(token_builder);
    if (token_builder->tokenInitialized())
        return;

    SipHash hash;
    for (const auto & colunm : chunk.getColumns())
        colunm->updateHashFast(hash);

    const auto hash_value = hash.get128();
    token_builder->setInitialToken(toString(hash_value.items[0]) + "_" + toString(hash_value.items[1]));
}

void SetUserTokenTransform::transform(Chunk & chunk)
{
    auto token_info = chunk.getChunkInfos().get<TokenInfo>();
    chassert(token_info);
    chassert(!token_info->tokenInitialized());
    token_info->setUserToken(user_token);
}

void SetSourceBlockNumberTransform::transform(Chunk & chunk)
{
    auto token_info = chunk.getChunkInfos().get<TokenInfo>();
    chassert(token_info);
    chassert(!token_info->tokenInitialized());
    token_info->setSourceBlockNumber(block_number++);
}

void SetMaterializeViewIDTransform::transform(Chunk & chunk)
{
    auto token_info = chunk.getChunkInfos().get<TokenInfo>();
    chassert(token_info);
    chassert(token_info->tokenInitialized());
    token_info->setMaterializeViewID(mv_id);
}

void SetMaterializeViewBlockNumberTransform::transform(Chunk & chunk)
{
    auto token_info = chunk.getChunkInfos().get<TokenInfo>();
    chassert(token_info);
    chassert(token_info->tokenInitialized());
    token_info->setMaterializeViewBlockNumber(block_number++);
}

void ResetTokenTransform::transform(Chunk & chunk)
{
    auto token_info = chunk.getChunkInfos().get<TokenInfo>();
    chassert(token_info);
    token_info->reset();
}

}
}
