#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace DB
{


/** OPTIMIZE query
  */
class ASTOptimizeQuery : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    /// The partition to optimize can be specified.
    IAST * partition = nullptr;
    /// A flag can be specified - perform optimization "to the end" instead of one step.
    bool final = false;
    /// Do deduplicate (default: false)
    bool deduplicate = false;
    /// Deduplicate by columns.
    IAST * deduplicate_by_columns = nullptr;
    /// Delete 'is_deleted' data
    bool cleanup = false;
    /** Get the text that identifies this element. */
    String getID(char delim) const override;

    ASTPtr clone() const override;

    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const override;

    QueryKind getQueryKind() const override { return QueryKind::Optimize; }

protected:
    void forEachPointerToChild(std::function<void(void**)> f) override;
};

}
