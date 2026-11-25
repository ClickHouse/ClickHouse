#pragma once

#include <Parsers/IAST.h>
#include <optional>

namespace Poco::JSON { class Object; }

namespace DB
{

/// Either a (possibly compound) expression representing a partition value, a partition ID, `ALL`,
/// or a `WHERE` predicate selecting partitions.
class ASTPartition : public IAST
{
public:
    IAST * value{nullptr};
    std::optional<size_t> fields_count;

    IAST * id{nullptr};
    bool all = false;

    /// `WHERE <predicate>`: every partition for which the predicate over the `_partition_id` and
    /// `_partition_value` virtual columns is true. `ParserAlterQuery` produces it only for
    /// `DROP PARTITION` and `DETACH PARTITION`.
    IAST * predicate{nullptr};

    String getID(char) const override;
    ASTPtr clone() const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    void setPartitionID(const ASTPtr & ast);
    void setPartitionValue(const ASTPtr & ast);
    void setPartitionPredicate(const ASTPtr & ast);

    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override
    {
        f(&value, nullptr);
        f(&id, nullptr);
        f(&predicate, nullptr);
    }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
