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
        bool tokenInitialized() const { return stage != INITIAL && stage != SOURCE_BLOCK_NUMBER; }

        void addPieceToInitialToken(String part);
        void closeInitialToken();
        void setUserToken(const String & token);
        void setSourceBlockNumber(size_t block_number);
        void setViewID(const String & id);
        void setViewBlockNumber(size_t block_number);
        void reset();

    private:
        String getTokenImpl() const;

        void addTokenPart(String part);
        size_t getTotalSize() const;

        /* Token has to be prepared in a particular order. BuildingStage ensure that token is expanded according the foloving order.
        * Firstly token has expand with information about the souce.
        * INITIAL -- in that stage token is expanded with several hash sums or with the user defined deduplication token.
        * SOURCE_BLOCK_NUMBER -- when token is expand with user defined deduplication token, after token has to be expanded with source block number.
        * After that token is considered as prepared for usage, hovewer it could be expanded with following details:
        * VIEW_ID -- in that stage token is expanded with view id, token could not be used until nex stage is passed.
        * VIEW_BLOCK_NUMBER - in that stage token is expanded with view block number.
        */
        enum BuildingStage
        {
            INITIAL,
            SOURCE_BLOCK_NUMBER,
            VIEW_ID,
            VIEW_BLOCK_NUMBER,
        };

        BuildingStage stage = INITIAL;
        std::vector<String> parts;
    };


#ifdef ABORT_ON_LOGICAL_ERROR
    /// use that class only with debug builds in CI for introspection
    class CheckTokenTransform : public ISimpleTransform
    {
    public:
        CheckTokenTransform(String debug_, bool must_be_present_, const Block & header_)
            : ISimpleTransform(header_, header_, true)
            , debug(std::move(debug_))
            , must_be_present(must_be_present_)
        {
        }

        String getName() const override { return "DeduplicationToken::CheckTokenTransform"; }

        void transform(Chunk & chunk) override;

    private:
        String debug;
        LoggerPtr log = getLogger("CheckInsertDeduplicationTokenTransform");
        bool must_be_present = false;
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


    class SetInitialTokenTransform : public ISimpleTransform
    {
    public:
        explicit SetInitialTokenTransform(const Block & header_)
            : ISimpleTransform(header_, header_, true)
        {
        }

        String getName() const override { return "DeduplicationToken::SetInitialTokenTransform"; }

        void transform(Chunk & chunk) override;

        static String getInitialToken(const Chunk & chunk);
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
