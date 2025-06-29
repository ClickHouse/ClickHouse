#pragma once

#include <Columns/IColumnDummy.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class ColumnStringAndSize final : public COWHelper<IColumnDummy, ColumnStringAndSize>
{
private:
    friend class COWHelper<IColumnDummy, ColumnStringAndSize>;

    explicit ColumnStringAndSize(
        ColumnPtr data_,
        ColumnPtr size_ = nullptr,
        ColumnPtr partial_data_ = nullptr,
        ColumnPtr partial_size_ = nullptr,
        size_t skipped_bytes_ = 0)
        : data(std::move(data_))
        , size(std::move(size_))
        , partial_data(std::move(partial_data_))
        , partial_size(std::move(partial_size_))
        , skipped_bytes(skipped_bytes_)
    {
    }

    ColumnPtr data;
    ColumnPtr size;
    ColumnPtr partial_data;
    ColumnPtr partial_size;
    size_t skipped_bytes = 0;

public:
    const char * getFamilyName() const override { return "StringAndSize"; }
    MutableColumnPtr cloneDummy(size_t) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "cloneDummy is not implemented for ColumnStringAndSize");
    }
    TypeIndex getDataType() const override { return TypeIndex::Nothing; }

    ColumnPtr getDataPtr() const { return data; }
    ColumnPtr getSizePtr() const { return size; }
    ColumnPtr getPartialDataPtr() const { return partial_data; }
    ColumnPtr getPartialSizePtr() const { return partial_size; }
    size_t getSkippedBytes() const { return skipped_bytes; }
};

}
