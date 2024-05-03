#pragma once

#include <Processors/Chunk.h>
#include <Processors/ISimpleTransform.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>

#include <memory>

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{
    struct SerialBlockNumberInfo : public ChunkInfoCloneable<SerialBlockNumberInfo>
    {
        SerialBlockNumberInfo(const SerialBlockNumberInfo & other) = default;
        explicit SerialBlockNumberInfo(size_t block_number_)
            : block_number(block_number_)
        {
        }

        size_t block_number = 0;
    };


    class NumberBlocksTransform : public ISimpleTransform
    {
    public:
        explicit NumberBlocksTransform(const Block & header)
            : ISimpleTransform(header, header, true)
        {
        }

        String getName() const override { return "NumberBlocksTransform"; }

        void transform(Chunk & chunk) override
        {
            chunk.getChunkInfos().add(std::make_shared<SerialBlockNumberInfo>(block_number++));
        }

    private:
        size_t block_number = 0;
    };


    class DedupTokenInfo : public ChunkInfoCloneable<DedupTokenInfo>
    {
    public:
        DedupTokenInfo() = default;
        DedupTokenInfo(const DedupTokenInfo & other) = default;
        explicit DedupTokenInfo(String first_part)
        {
            addTokenPart(std::move(first_part));
        }

        String getToken() const
        {
            String result;
            result.reserve(getTotalSize());

            for (const auto & part : token_parts)
            {
                result.append(part);
            }

            return result;
        }

        bool empty() const
        {
            return token_parts.empty();
        }

        void addTokenPart(String part)
        {
            if (!part.empty())
                token_parts.push_back(std::move(part));
        }

    private:
        size_t getTotalSize() const
        {
            size_t size = 0;
            for (const auto & part : token_parts)
                size += part.size();
            return size;
        }

        std::vector<String> token_parts;
    };

    class AddUserDeduplicationTokenTransform : public ISimpleTransform
    {
    public:
        AddUserDeduplicationTokenTransform(String token_, const Block & header_)
            : ISimpleTransform(header_, header_, true)
            , token(token_)
        {
        }

        String getName() const override { return "AddUserDeduplicationTokenTransform"; }

        void transform(Chunk & chunk) override
        {
            chunk.getChunkInfos().add(std::make_shared<DedupTokenInfo>(token));
        }

    private:
        String token;
    };


    class CheckInsertDeduplicationTokenTransform : public ISimpleTransform
    {
    public:
        CheckInsertDeduplicationTokenTransform(String debug_, bool must_be_present_, const Block & header_)
            : ISimpleTransform(header_, header_, true)
            , debug(debug_)
            , must_be_present(must_be_present_)
        {
        }

        String getName() const override { return "CheckInsertDeduplicationTokenTransform"; }

        void transform(Chunk & chunk) override
        {
            if (!must_be_present)
                return;

            auto token_info = chunk.getChunkInfos().get<DedupTokenInfo>();
            if (!token_info)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk has to have DedupTokenInfo as ChunkInfo, {}", debug);

            LOG_DEBUG(getLogger("CheckInsertDeduplicationTokenTransform"),
                "{}, token: {}",
                debug, token_info->getToken());
        }

    private:
        String debug;
        bool must_be_present = false;
    };


    class ExtendDeduplicationWithBlockNumberFromInfoTokenTransform : public ISimpleTransform
    {
    public:
        explicit ExtendDeduplicationWithBlockNumberFromInfoTokenTransform(const Block & header_)
                : ISimpleTransform(header_, header_, true)
        {
        }

        String getName() const override { return "ExtendDeduplicationWithBlockNumberFromInfoTokenTransform"; }

        void transform(Chunk & chunk) override
        {
            auto token_info = chunk.getChunkInfos().get<DedupTokenInfo>();
            if (!token_info)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk has to have DedupTokenInfo as ChunkInfo, recs {}", chunk.getChunkInfos().size());

            auto block_number_info = chunk.getChunkInfos().get<SerialBlockNumberInfo>();
            if (!block_number_info)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk has to have SerialBlockNumberInfo as ChunkInfo");

            token_info->addTokenPart(fmt::format(":block-{}", block_number_info->block_number));

            LOG_DEBUG(getLogger("ExtendDeduplicationWithBlockNumberFromInfoTokenTransform"),
                "updated with {}, result: {}",
                fmt::format(":block-{}", block_number_info->block_number), token_info->getToken());
        }
    };

    class ExtendDeduplicationWithBlockNumberTokenTransform : public ISimpleTransform
    {
    public:
        explicit ExtendDeduplicationWithBlockNumberTokenTransform(const Block & header_)
                : ISimpleTransform(header_, header_, true)
        {
        }

        String getName() const override { return "ExtendDeduplicationWithBlockNumberTokenTransform"; }

        void transform(Chunk & chunk) override
        {
            auto token_info = chunk.getChunkInfos().get<DedupTokenInfo>();
            if (!token_info)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk has to have DedupTokenInfo as ChunkInfo");

            auto x = block_number++;
            token_info->addTokenPart(fmt::format(":block-{}", x));

            LOG_DEBUG(getLogger("ExtendDeduplicationWithBlockNumberTokenTransform"),
                "updated with {}, result: {}",
                fmt::format(":block-{}", x), token_info->getToken());
        }
    private:
        size_t block_number = 0;
    };

    class ExtendDeduplicationWithTokenPartTransform : public ISimpleTransform
    {
    public:
        ExtendDeduplicationWithTokenPartTransform(String token_part_, const Block & header_)
                : ISimpleTransform(header_, header_, true)
                , token_part(token_part_)
        {
        }

        String getName() const override { return "ExtendDeduplicationWithBlockNumberTokenTransform"; }

        void transform(Chunk & chunk) override
        {
            auto token_info = chunk.getChunkInfos().get<DedupTokenInfo>();
            if (!token_info)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk has to have DedupTokenInfo as ChunkInfo, try to add token part {}", token_part);

            token_info->addTokenPart(fmt::format("{}", token_part));

            LOG_DEBUG(getLogger("ExtendDeduplicationWithTokenPartTransform"),
                "updated with {}, result: {}",
                token_part, token_info->getToken());
        }

    private:
        String token_part;
    };

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

}
