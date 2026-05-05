#pragma once

#include <Parsers/IAST.h>
#include <optional>

namespace DB
{

/// Either a (possibly compound) expression representing a partition value or a partition ID.
class ASTPartition : public IAST
{
public:
    IAST * value{nullptr};
    std::optional<size_t> fields_count;

    IAST * id{nullptr};
    bool all = false;

    String getID(char) const override;
    ASTPtr clone() const override;

    void setPartitionID(const ASTPtr & ast);
    void setPartitionValue(const ASTPtr & ast);

    void forEachPointerToChild(std::function<void(void **)> f) override
    {
        f(reinterpret_cast<void **>(&value));
        f(reinterpret_cast<void **>(&id));
    }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
