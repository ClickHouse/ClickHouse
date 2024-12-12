#pragma once


#include "Parsers/IAST.h"
#include "Parsers/IAST_fwd.h"
namespace DB::GoogleSQL
{

    class ASTPipelineQuery: public DB::IAST
    {
    public:
        enum class StageType
        {
           FROM,
           SELECT,
        };

        struct PipelineStage
        {
            StageType type;
            ASTPtr expression;
        };

        std::vector<PipelineStage> stages;

        String getID([[maybe_unused]] char delimiter) const override { return "PipelineQuery"; }
        ASTPtr clone() const override;
    };

}
