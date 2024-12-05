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
    chunk.getChunkInfos().appendIfUniq(chunk_infos.clone());
}

namespace DeduplicationToken
{

String TokenInfo::getToken() const
{
    if (!isDefined())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "token is not defined, stage {}, token {}", stage, debugToken());

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

void TokenInfo::addChunkHash(String part)
{
    if (stage == UNDEFINED && empty())
        stage = DEFINE_SOURCE_WITH_HASHES;

    if (stage != DEFINE_SOURCE_WITH_HASHES)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "token is in wrong stage {}, token {}", stage, debugToken());

    addTokenPart(std::move(part));
}

void TokenInfo::finishChunkHashes()
{
    if (stage == UNDEFINED && empty())
        stage = DEFINE_SOURCE_WITH_HASHES;

    if (stage != DEFINE_SOURCE_WITH_HASHES)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "token is in wrong stage {}, token {}", stage, debugToken());

    stage = DEFINED;
}

void TokenInfo::setUserToken(const String & token)
{
    if (stage == UNDEFINED && empty())
        stage = DEFINE_SOURCE_USER_TOKEN;

    if (stage != DEFINE_SOURCE_USER_TOKEN)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "token is in wrong stage {}, token {}", stage, debugToken());

    addTokenPart(fmt::format("user-token-{}", token));
}

void TokenInfo::setSourceWithUserToken(size_t block_number)
{
    if (stage != DEFINE_SOURCE_USER_TOKEN)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "token is in wrong stage {}, token {}", stage, debugToken());

    addTokenPart(fmt::format("source-number-{}", block_number));

    stage = DEFINED;
}

void TokenInfo::setViewID(const String & id)
{
    if (stage == DEFINED)
        stage = DEFINE_VIEW;

    if (stage != DEFINE_VIEW)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "token is in wrong stage {}, token {}", stage, debugToken());

    addTokenPart(fmt::format("view-id-{}", id));
}

void TokenInfo::setViewBlockNumber(size_t block_number)
{
    if (stage != DEFINE_VIEW)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "token is in wrong stage {}, token {}", stage, debugToken());

    addTokenPart(fmt::format("view-block-{}", block_number));

    stage = DEFINED;
}

void TokenInfo::reset()
{
    stage = UNDEFINED;
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

    // we reserve more size here to be able to add delimenter between parts.
    return size + parts.size() - 1;
}

#ifdef ABORT_ON_LOGICAL_ERROR
void CheckTokenTransform::transform(Chunk & chunk)
{
    auto token_info = chunk.getChunkInfos().get<TokenInfo>();

    if (!token_info)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk has to have DedupTokenInfo as ChunkInfo, {}", debug);
    }

    LOG_TEST(log, "debug: {}, token: {}, columns {} rows {}", debug, token_info->debugToken(), chunk.getNumColumns(), chunk.getNumRows());
}
#endif

String DefineSourceWithChunkHashTransform::getChunkHash(const Chunk & chunk)
{
    SipHash hash;
    for (const auto & colunm : chunk.getColumns())
        colunm->updateHashFast(hash);

    const auto hash_value = hash.get128();
    return toString(hash_value.items[0]) + "_" + toString(hash_value.items[1]);
}


void DefineSourceWithChunkHashTransform::transform(Chunk & chunk)
{
    auto token_info = chunk.getChunkInfos().get<TokenInfo>();

    if (!token_info)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "TokenInfo is expected for consumed chunk in DefineSourceWithChunkHashesTransform");

    if (token_info->isDefined())
        return;

    token_info->addChunkHash(getChunkHash(chunk));
    token_info->finishChunkHashes();
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
    token_info->setSourceWithUserToken(block_number++);
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
