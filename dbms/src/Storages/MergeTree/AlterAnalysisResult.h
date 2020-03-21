#pragma once
#include <Interpreters/ExpressionActions.h>

namespace DB
{
    struct AlterAnalysisResult
    {
        /// Expression for column type conversion.
        /// If no conversions are needed, expression=nullptr.
        ExpressionActionsPtr expression = nullptr;

        /// Denotes if metadata must be changed even if no file should be overwritten
        /// (used for transformation-free changing of Enum values list).
        bool force_update_metadata = false;

        std::map<String, const IDataType *> new_types;

        /// For every column that need to be converted: source column name,
        ///  column name of calculated expression for conversion.
        std::vector<std::pair<String, String>> conversions;
        NamesAndTypesList removed_columns;
        Names removed_indices;
    };
}
