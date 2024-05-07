#pragma once

#include <Processors/Chunk.h>
#include <Processors/ISimpleTransform.h>

#include <base/defines.h>


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{
    class RestoreChunkInfosTransform : public ISimpleTransform
    {
    public:
        RestoreChunkInfosTransform(Chunk::ChunkInfoCollection chunk_infos_, const Block & header_)
                : ISimpleTransform(header_, header_, true)
                , chunk_infos(chunk_infos_)
        {
        }

        String getName() const override { return "RestoreChunkInfosTransform"; }

        void transform(Chunk & chunk) override
        {
            chunk.getChunkInfos().append(chunk_infos.clone());
        }

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

        String getToken(bool enable_assert = true) const;

        bool empty() const { return parts.empty(); }
        bool tokenInitialized() const { return stage != INITIAL && stage != SOURCE_BLOCK_NUMBER; }

        void setInitialToken(String part);
        void setUserToken(const String & token);
        void setSourceBlockNumber(size_t sbn);
        void setMaterializeViewID(const String & id);
        void setMaterializeViewBlockNumber(size_t mvbn);
        void reset();

    private:
        void addTokenPart(String part);
        size_t getTotalSize() const;

        enum BuildingStage
        {
            INITIAL,
            SOURCE_BLOCK_NUMBER,
            MATERIALIZE_VIEW_ID,
            MATERIALIZE_VIEW_BLOCK_NUMBER,
        };

        BuildingStage stage = INITIAL;
        std::vector<String> parts;
    };


    class CheckTokenTransform : public ISimpleTransform
    {
    public:
        CheckTokenTransform(String debug_, bool must_be_present_, const Block & header_)
            : ISimpleTransform(header_, header_, true)
            , debug(debug_)
            , must_be_present(must_be_present_)
        {
        }

        String getName() const override { return "DeduplicationToken::CheckTokenTransform"; }

        void transform(Chunk & chunk) override;

    private:
        String debug;
        bool must_be_present = false;
    };


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
        size_t block_number;
    };


    class SetMaterializeViewIDTransform : public ISimpleTransform
    {
    public:
        SetMaterializeViewIDTransform(String mv_id_, const Block & header_)
            : ISimpleTransform(header_, header_, true)
            , mv_id(std::move(mv_id_))
        {
        }

        String getName() const override { return "DeduplicationToken::SetMaterializeViewIDTransform"; }

        void transform(Chunk & chunk) override;

    private:
        String mv_id;
    };


    class SetMaterializeViewBlockNumberTransform : public ISimpleTransform
    {
    public:
        explicit SetMaterializeViewBlockNumberTransform(const Block & header_)
            : ISimpleTransform(header_, header_, true)
        {
        }

        String getName() const override { return "DeduplicationToken::SetMaterializeViewBlockNumberTransform"; }

        void transform(Chunk & chunk) override;

    private:
        size_t block_number;
    };

}
}
