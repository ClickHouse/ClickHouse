#include <Processors/Transforms/HotKeyState.h>

#include <QueryPipeline/SizeLimits.h>

namespace DB
{

HotKeyState::HotKeyState(ColumnsWithTypeAndName key_header_)
    : key_header(std::move(key_header_))
{
    for (const auto & column : key_header)
        accumulated_keys.push_back(column.type->createColumn());
}

ColumnPtr HotKeyState::buildHotMask(const Columns & key_columns) const
{
    auto set = snapshot.get();
    if (!set || set->empty())
        return nullptr;

    ColumnsWithTypeAndName args;
    args.reserve(key_header.size());
    for (size_t k = 0; k < key_header.size(); ++k)
        args.emplace_back(key_columns[k], key_header[k].type, key_header[k].name);

    return set->execute(args, false);
}

void HotKeyState::promote(const Columns & key_columns, size_t row, UInt32 hash)
{
    std::lock_guard lock(mutex);

    if (!promoted_hashes.insert(hash).second)
        return;

    for (size_t k = 0; k < accumulated_keys.size(); ++k)
        accumulated_keys[k]->insertFrom(*key_columns[k], row);

    /// The hot set holds only a few keys, so there is no need to limit its size; an empty `SizeLimits`
    /// means unlimited.
    auto set = std::make_unique<Set>(SizeLimits{}, 0, /*transform_null_in=*/true);
    set->setHeader(key_header);

    Columns columns;
    columns.reserve(accumulated_keys.size());
    for (const auto & column : accumulated_keys)
        columns.push_back(column->cloneResized(column->size()));

    set->insertFromColumns(columns);
    set->finishInsert();

    snapshot.set(std::unique_ptr<const Set>(std::move(set)));
}

}
