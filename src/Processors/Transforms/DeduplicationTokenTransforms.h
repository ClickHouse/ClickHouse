#pragma once

#include <Processors/Chunk.h>
#include <Processors/ISimpleTransform.h>

#include <base/defines.h>
#include "Common/Logger.h"


namespace DB
{
    class RestoreChunkInfosTransform : public ISimpleTransform
    {
    public:
        RestoreChunkInfosTransform(Chunk::ChunkInfoCollection chunk_infos_, const Block & header_)
                : ISimpleTransform(header_, header_, true)
                , chunk_infos(std::move(chunk_infos_))
        {}

        String getName() const override { return "RestoreChunkInfosTransform"; }

        void transform(Chunk & chunk) override;

    private:
        Chunk::ChunkInfoCollection chunk_infos;
    };


namespace DeduplicationToken
{
    class TokenInfo : public ChunkInfoCloneable<TokenInfo>
    {
    public:
        TokenInfo() = default;
        TokenInfo(const TokenInfo & other) = default;

        String getToken() const;
        String debugToken() const;

        bool empty() const { return parts.empty(); }

        bool isDefined() const { return stage == DEFINED; }

        void addChunkHash(String part);
        void finishChunkHashes();

        void setUserToken(const String & token);
        void setSourceWithUserToken(size_t block_number);

        void setViewID(const String & id);
        void setViewBlockNumber(size_t block_number);

        void reset();

    private:
        String getTokenImpl() const;

        void addTokenPart(String part);
        size_t getTotalSize() const;

        /* Token has to be prepared in a particular order.
        * BuildingStage ensures that token is expanded according the following order.
        * Firstly token is expanded with information about the source.
        * It could be done with two ways: add several hash sums from the source chunks or provide user defined deduplication token and its sequentional block number.
        *
        * transition // method
        * UNDEFINED -> DEFINE_SOURCE_WITH_HASHES // addChunkHash
        * DEFINE_SOURCE_WITH_HASHES -> DEFINE_SOURCE_WITH_HASHES // addChunkHash
        * DEFINE_SOURCE_WITH_HASHES -> DEFINED // defineSourceWithChankHashes
        *
        * transition // method
        * UNDEFINED -> DEFINE_SOURCE_USER_TOKEN // setUserToken
        * DEFINE_SOURCE_USER_TOKEN -> DEFINED // defineSourceWithUserToken
        *
        * After token is defined, it could be extended with view id and view block number. Actually it has to be expanded with view details if there is one or several views.
        *
        * transition // method
        * DEFINED -> DEFINE_VIEW // setViewID
        * DEFINE_VIEW -> DEFINED // defineViewID
        */

        enum BuildingStage
        {
            UNDEFINED,
            DEFINE_SOURCE_WITH_HASHES,
            DEFINE_SOURCE_USER_TOKEN,
            DEFINE_VIEW,
            DEFINED,
        };

        BuildingStage stage = UNDEFINED;
        std::vector<String> parts;
    };


#ifdef ABORT_ON_LOGICAL_ERROR
    /// use that class only with debug builds in CI for introspection
    class CheckTokenTransform : public ISimpleTransform
    {
    public:
        CheckTokenTransform(String debug_, const Block & header_)
            : ISimpleTransform(header_, header_, true)
            , debug(std::move(debug_))
        {
        }

        String getName() const override { return "DeduplicationToken::CheckTokenTransform"; }

        void transform(Chunk & chunk) override;

    private:
        String debug;
        LoggerPtr log = getLogger("CheckInsertDeduplicationTokenTransform");
    };
#endif


    class AddTokenInfoTransform : public ISimpleTransform
    {
    public:
        explicit AddTokenInfoTransform(const Block & header_)
            : ISimpleTransform(header_, header_, true)
        {
        }

        String getName() const override { return "DeduplicationToken::AddTokenInfoTransform"; }

        void transform(Chunk & chunk) override
        {
            chunk.getChunkInfos().add(std::make_shared<TokenInfo>());
        }
    };


    class DefineSourceWithChunkHashTransform : public ISimpleTransform
    {
    public:
        explicit DefineSourceWithChunkHashTransform(const Block & header_)
            : ISimpleTransform(header_, header_, true)
        {
        }

        String getName() const override { return "DeduplicationToken::DefineSourceWithChunkHashesTransform"; }

        // Usually MergeTreeSink/ReplicatedMergeTreeSink calls addChunkHash for the deduplication token with hashes from the parts.
        // But if there is some table with different engine, we still need to define the source of the data in deduplication token
        // We use that transform to define the source as a hash of entire block in deduplication token
        void transform(Chunk & chunk) override;

        static String getChunkHash(const Chunk & chunk);
    };

    class ResetTokenTransform : public ISimpleTransform
    {
    public:
        explicit ResetTokenTransform(const Block & header_)
            : ISimpleTransform(header_, header_, true)
        {
        }

        String getName() const override { return "DeduplicationToken::ResetTokenTransform"; }

        void transform(Chunk & chunk) override;
    };


    class SetUserTokenTransform : public ISimpleTransform
    {
    public:
        SetUserTokenTransform(String user_token_, const Block & header_)
            : ISimpleTransform(header_, header_, true)
            , user_token(std::move(user_token_))
        {
        }

        String getName() const override { return "DeduplicationToken::SetUserTokenTransform"; }

        void transform(Chunk & chunk) override;

    private:
        String user_token;
    };


    class SetSourceBlockNumberTransform : public ISimpleTransform
    {
    public:
        explicit SetSourceBlockNumberTransform(const Block & header_)
            : ISimpleTransform(header_, header_, true)
        {
        }

        String getName() const override { return "DeduplicationToken::SetSourceBlockNumberTransform"; }

        void transform(Chunk & chunk) override;

    private:
        size_t block_number = 0;
    };


    class SetViewIDTransform : public ISimpleTransform
    {
    public:
        SetViewIDTransform(String view_id_, const Block & header_)
            : ISimpleTransform(header_, header_, true)
            , view_id(std::move(view_id_))
        {
        }

        String getName() const override { return "DeduplicationToken::SetViewIDTransform"; }

        void transform(Chunk & chunk) override;

    private:
        String view_id;
    };


    class SetViewBlockNumberTransform : public ISimpleTransform
    {
    public:
        explicit SetViewBlockNumberTransform(const Block & header_)
            : ISimpleTransform(header_, header_, true)
        {
        }

        String getName() const override { return "DeduplicationToken::SetViewBlockNumberTransform"; }

        void transform(Chunk & chunk) override;

    private:
        size_t block_number = 0;
    };

}
}
