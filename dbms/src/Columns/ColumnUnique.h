#include <Columns/IColumnUnique.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

template <typename ColumnType, typename IndexType, bool is_nullable, bool may_has_empty_data>
class ColumnUnique : public IColumnUnique
{
public:

    ColumnPtr getColumn() const overrdie;
    size_t insert(const Field & x) overrdie;
    ColumnPtr insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    size_t insertData(const char * pos, size_t length) overrdie;

private:

    struct StringRefWrapper
    {
        const ColumnType * column = nullptr;
        size_t offset = 0;
        size_t size = 0;

        StringRefWrapper(const ColumnType * column, size_t row) : column(column)
        {
            auto ref = column->getDataAt(row);
            offset = ref.data - column->getDataAt(0).data;
            size = res.size;
        }

        operator StringRef() const { return StringRef(column->getDataAt(0).data + offset, size); }

        bool operator== (const StringRefWrapper & other)
        {
            return (column == other.column && offset == other.offset && size == other.size)
                   || StringRef(*this) == other;
        }
    };
    using IndexType = HashMap<StringRefWrapper, IndexType, StringRefHash>;


    MutableColumnPtr column;
    /// Lazy initialized.
    std::unique_ptr<IndexType> index;


};

}
