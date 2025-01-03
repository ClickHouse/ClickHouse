#pragma once


#include "Parsers/IAST.h"
#include "Parsers/IAST_fwd.h"
namespace DB::ZetaSQL
{

    class ASTZetaSQLQuery: public IAST
    {
    public:
        enum class StageKeyword
        {
           FROM,
           SELECT,
           AGGREGATE,
           WHERE,
        };

        struct PipelineStage
        {
            StageKeyword type;
            ASTPtr expression;
        };

        std::list<PipelineStage> stages;

        String getID([[maybe_unused]] char delimiter) const override { return "ZetaSQLQuery"; }
        ASTPtr clone() const override;
    };

}
