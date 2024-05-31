#include <Processors/Transforms/DeduplicationTokenTransforms.h>

#include <IO/WriteHelpers.h>

#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>


#include <fmt/core.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void RestoreChunkInfosTransform::transform(Chunk & chunk)
{
    LOG_TRACE(getLogger("RestoreChunkInfosTransform"), "chunk infos before: {}:{}, append: {}:{}, chunk has rows {}",
        chunk.getChunkInfos().size(), chunk.getChunkInfos().debug(),
        chunk_infos.size(), chunk_infos.debug(),
        chunk.getNumRows());

    chunk.getChunkInfos().append(chunk_infos.clone());
}

namespace DeduplicationToken
{

String DB::DeduplicationToken::TokenInfo::getToken(bool enable_assert) const
{
    chassert(stage == VIEW_ID || !enable_assert);

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
    stage = VIEW_ID;
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
    stage = VIEW_ID;
}

void TokenInfo::setViewID(const String & id)
{
    chassert(stage == VIEW_ID);
    addTokenPart(fmt::format(":view-id-{}", id));
    stage = VIEW_BLOCK_NUMBER;
}

void TokenInfo::setViewBlockNumber(size_t mvbn)
{
    chassert(stage == VIEW_BLOCK_NUMBER);
    addTokenPart(fmt::format(":view-block-{}", mvbn));
    stage = VIEW_ID;
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

void SetInitialTokenTransform::setInitialToken(Chunk & chunk)
{
    auto token_info = chunk.getChunkInfos().get<TokenInfo>();

    LOG_DEBUG(getLogger("SetInitialTokenTransform"), "has token_info {}", bool(token_info));

    if (!token_info)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "TokenInfo is expected for consumed chunk in SetInitialTokenTransform");

    if (token_info->tokenInitialized())
        return;

    SipHash hash;
    for (const auto & colunm : chunk.getColumns())
        colunm->updateHashFast(hash);

    const auto hash_value = hash.get128();
    token_info->setInitialToken(toString(hash_value.items[0]) + "_" + toString(hash_value.items[1]));
}


void SetInitialTokenTransform::transform(Chunk & chunk)
{
    setInitialToken(chunk);
}

void SetUserTokenTransform::transform(Chunk & chunk)
{
    auto token_info = chunk.getChunkInfos().get<TokenInfo>();
    if (!token_info)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "TokenInfo is expected for consumed chunk in SetUserTokenTransform");
    token_info->setUserToken(user_token);
}

void SetSourceBlockNumberTransform::transform(Chunk & chunk)
{
    auto token_info = chunk.getChunkInfos().get<TokenInfo>();
    if (!token_info)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "TokenInfo is expected for consumed chunk in SetSourceBlockNumberTransform");
    token_info->setSourceBlockNumber(block_number++);
}

void SetViewIDTransform::transform(Chunk & chunk)
{
    auto token_info = chunk.getChunkInfos().get<TokenInfo>();
    if (!token_info)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "TokenInfo is expected for consumed chunk in SetViewIDTransform");
    token_info->setViewID(view_id);
}

void SetViewBlockNumberTransform::transform(Chunk & chunk)
{
    auto token_info = chunk.getChunkInfos().get<TokenInfo>();
    if (!token_info)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "TokenInfo is expected for consumed chunk in SetViewBlockNumberTransform");
    token_info->setViewBlockNumber(block_number++);
}

void ResetTokenTransform::transform(Chunk & chunk)
{
    auto token_info = chunk.getChunkInfos().get<TokenInfo>();
    if (!token_info)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "TokenInfo is expected for consumed chunk in ResetTokenTransform");

    LOG_DEBUG(getLogger("ResetTokenTransform"), "token_info was {}", token_info->getToken(false));
    token_info->reset();
}

}
}
