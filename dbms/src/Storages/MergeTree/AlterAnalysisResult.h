#pragma once
#include <Interpreters/ExpressionActions.h>

namespace DB
{
    struct AlterAnalysisResult
    {
        ExpressionActionsPtr expression = nullptr;
        bool force_update_metadata = false;
        std::map<String, const IDataType *> new_types;
        /// For every column that need to be converted: source column name,
        ///  column name of calculated expression for conversion.
        std::vector<std::pair<String, String>> conversions;
        NamesAndTypesList removed_columns;
        Names removed_indices;
    };
}
