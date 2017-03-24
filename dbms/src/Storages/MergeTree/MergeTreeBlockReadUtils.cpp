#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace DB {


NameSet injectRequiredColumns(const MergeTreeData & storage, const MergeTreeData::DataPartPtr & part, Names & columns)
{
    NameSet required_columns{std::begin(columns), std::end(columns)};
    NameSet injected_columns;

    auto all_column_files_missing = true;

    for (size_t i = 0; i < columns.size(); ++i)
    {
        const auto & column_name = columns[i];

        /// column has files and hence does not require evaluation
        if (part->hasColumnFiles(column_name))
        {
            all_column_files_missing = false;
            continue;
        }

        const auto default_it = storage.column_defaults.find(column_name);
        /// columns has no explicit default expression
        if (default_it == std::end(storage.column_defaults))
            continue;

        /// collect identifiers required for evaluation
        IdentifierNameSet identifiers;
        default_it->second.expression->collectIdentifierNames(identifiers);

        for (const auto & identifier : identifiers)
        {
            if (storage.hasColumn(identifier))
            {
                /// ensure each column is added only once
                if (required_columns.count(identifier) == 0)
                {
                    columns.emplace_back(identifier);
                    required_columns.emplace(identifier);
                    injected_columns.emplace(identifier);
                }
            }
        }
    }

    /** Add a column of the minimum size.
        * Used in case when no column is needed or files are missing, but at least you need to know number of rows.
        * Adds to the columns.
        */
    if (all_column_files_missing)
    {
        const auto minimum_size_column_name = part->getColumnNameWithMinumumCompressedSize();
        columns.push_back(minimum_size_column_name);
        /// correctly report added column
        injected_columns.insert(columns.back());
    }

    return injected_columns;
}


}
