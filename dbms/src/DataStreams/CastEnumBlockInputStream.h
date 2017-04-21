#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataTypes/DataTypeEnum.h>

#include <experimental/optional>
#include <vector>

namespace DB
{

class CastEnumBlockInputStream : public IProfilingBlockInputStream
{
public:
    CastEnumBlockInputStream(BlockInputStreamPtr input_,
                             const Block & in_sample_,
                             const Block & out_sample_)
    {
        collectEnums(in_sample_, out_sample_);
        children.push_back(input_);
    }

    String getName() const override { return "CastEnumBlockInputStream"; }

    String getID() const override
    {
        std::stringstream res;
        res << "CastEnumBlockInputStream(" << children.back()->getID() << ")";
        return res.str();
    }

protected:
    Block readImpl() override
    {
        Block block = children.back()->read();

        if (!block || enum_types.empty())
            return block;

        Block res;
        size_t s = block.columns();

        for (size_t i = 0; i < s; ++i)
        {
            const auto & elem = block.getByPosition(i);

            if (bool(enum_types[i]))
            {
                const auto & type = static_cast<const IDataTypeEnum*>(enum_types[i]->type.get());
                ColumnPtr new_column = type->createColumn();

                for (size_t j = 0; j < elem.column->size(); ++j)
                    new_column->insert(type->castToValue((*elem.column)[j]));

                res.insert({
                    new_column,
                    enum_types[i]->type,
                    enum_types[i]->name}
                );
            }
            else
            {
                res.insert(elem);
            }
        }

        return res;
    }

private:
    void collectEnums(const Block & in_sample, const Block & out_sample)
    {
        size_t in_size = in_sample.columns();
        for (size_t i = 0; i < in_size; ++i)
        {
            const auto & in_elem  = in_sample.getByPosition(i);
            const auto & out_elem = out_sample.getByPosition(i);

            /// Force conversion only if source type is not Enum.
            if ( dynamic_cast<IDataTypeEnum*>(out_elem.type.get()) &&
                !dynamic_cast<IDataTypeEnum*>(in_elem.type.get()))
            {
                enum_types.push_back(NameAndTypePair(out_elem.name, out_elem.type));
            }
            else
            {
                enum_types.push_back(std::experimental::nullopt);
            }
        }
    }

private:
    std::vector<std::experimental::optional<NameAndTypePair>> enum_types;
};

}
