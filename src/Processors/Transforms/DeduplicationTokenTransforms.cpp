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
    chunk.getChunkInfos().append(chunk_infos.clone());
}

namespace DeduplicationToken
{

String TokenInfo::getToken() const
{
    if (stage != VIEW_ID)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "token is in wrong stage {}, token {}", stage, debugToken());

    return getTokenImpl();
}

String TokenInfo::getTokenImpl() const
{
    String result;
    result.reserve(getTotalSize());

    for (const auto & part : parts)
    {
        if (!result.empty())
            result.append(":");
        result.append(part);
    }

    return result;
}

String TokenInfo::debugToken() const
{
    return getTokenImpl();
}


void TokenInfo::addPieceToInitialToken(String part)
{
    if (stage != INITIAL)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "token is in wrong stage {}, token {}", stage, debugToken());
    addTokenPart(std::move(part));
}

void TokenInfo::closeInitialToken()
{
    chassert(stage == INITIAL);
    stage = VIEW_ID;
}

void TokenInfo::setUserToken(const String & token)
{
    if (stage != INITIAL)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "token is in wrong stage {}, token {}", stage, debugToken());

    addTokenPart(fmt::format("user-token-{}", token));
    stage = SOURCE_BLOCK_NUMBER;
}

void TokenInfo::setSourceBlockNumber(size_t block_number)
{
    if (stage != SOURCE_BLOCK_NUMBER)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "token is in wrong stage {}, token {}", stage, debugToken());

    addTokenPart(fmt::format("source-number-{}", block_number));
    stage = VIEW_ID;
}

void TokenInfo::setViewID(const String & id)
{
    if (stage != VIEW_ID)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "token is in wrong stage {}, token {}", stage, debugToken());

    addTokenPart(fmt::format("view-id-{}", id));
    stage = VIEW_BLOCK_NUMBER;
}

void TokenInfo::setViewBlockNumber(size_t block_number)
{
    if (stage != VIEW_BLOCK_NUMBER)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "token is in wrong stage {}, token {}", stage, debugToken());

    addTokenPart(fmt::format("view-block-{}", block_number));
    stage = VIEW_ID;
}

void TokenInfo::reset()
{
    stage = INITIAL;
    parts.clear();
}

void TokenInfo::addTokenPart(String part)
{
    parts.push_back(std::move(part));
}

size_t TokenInfo::getTotalSize() const
{
    if (parts.empty())
        return 0;

    size_t size = 0;
    for (const auto & part : parts)
        size += part.size();

    return size + parts.size() - 1;
}

#ifdef ABORT_ON_LOGICAL_ERROR
void CheckTokenTransform::transform(Chunk & chunk)
{
    auto token_info = chunk.getChunkInfos().get<TokenInfo>();

    if (!token_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk has to have DedupTokenInfo as ChunkInfo, {}", debug);

    if (!must_be_present)
    {
        LOG_DEBUG(log, "{}, no token required, token {}", debug, token_info->debugToken());
        return;
    }

    LOG_DEBUG(log, "{}, token: {}", debug, token_info->debugToken());
}
#endif

String SetInitialTokenTransform::getInitialToken(const Chunk & chunk)
{
    SipHash hash;
    for (const auto & colunm : chunk.getColumns())
        colunm->updateHashFast(hash);

    const auto hash_value = hash.get128();
    return toString(hash_value.items[0]) + "_" + toString(hash_value.items[1]);
}


void SetInitialTokenTransform::transform(Chunk & chunk)
{
    auto token_info = chunk.getChunkInfos().get<TokenInfo>();

    if (!token_info)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "TokenInfo is expected for consumed chunk in SetInitialTokenTransform");

    if (token_info->tokenInitialized())
        return;

    token_info->addPieceToInitialToken(getInitialToken(chunk));
    token_info->closeInitialToken();
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

    token_info->reset();
}

}
}
