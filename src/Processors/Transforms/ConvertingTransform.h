#include <Processors/ISimpleTransform.h>
#include <Core/ColumnNumbers.h>

namespace DB
{

/** Convert one block structure to another:
  *
  * Leaves only necessary columns;
  *
  * Columns are searched in source first by name;
  *  and if there is no column with same name, then by position.
  *
  * Converting types of matching columns (with CAST function).
  *
  * Materializing columns which are const in source and non-const in result,
  *  throw if they are const in result and non const in source,
  *   or if they are const and have different values.
  */
class ConvertingTransform : public ISimpleTransform
{
public:
    enum class MatchColumnsMode
    {
        /// Require same number of columns in source and result. Match columns by corresponding positions, regardless to names.
        Position,
        /// Find columns in source by their names. Allow excessive columns in source.
        Name,
    };

    ConvertingTransform(
        Block source_header_,
        Block result_header_,
        MatchColumnsMode mode_);

    String getName() const override { return "Converting"; }

protected:
    void transform(Chunk & chunk) override;

private:
    /// How to construct result block. Position in source block, where to get each column.
    ColumnNumbers conversion;
};

}
