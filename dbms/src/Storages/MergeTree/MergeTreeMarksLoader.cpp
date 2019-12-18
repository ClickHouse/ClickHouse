#include <Storages/MergeTree/MergeTreeMarksLoader.h>

namespace DB
{

MergeTreeMarksLoader::MergeTreeMarksLoader(
    MarkCache * mark_cache_,
    const String & mrk_path_,
    const LoadFunc & load_func_,
    bool save_marks_in_cache_,
    size_t columns_num_)
    : mark_cache(mark_cache_)
    , mrk_path(mrk_path_)
    , load_func(load_func_)
    , save_marks_in_cache(save_marks_in_cache_)
    , columns_num(columns_num_) {}

const MarkInCompressedFile & MergeTreeMarksLoader::getMark(size_t row_index, size_t column_index)
{
    if (!marks)
        loadMarks();
    if (column_index >= columns_num)
        throw Exception("Column index: " + toString(column_index) 
            + " is out of range (" + toString(columns_num) + ")", ErrorCodes::LOGICAL_ERROR);
    
    return (*marks)[row_index * columns_num + column_index];
}

void MergeTreeMarksLoader::loadMarks()
{
    auto load = std::bind(load_func, mrk_path);
    if (mark_cache)
    {
        auto key = mark_cache->hash(mrk_path);
        if (save_marks_in_cache)
        {
            marks = mark_cache->getOrSet(key, load);
        }
        else
        {
            marks = mark_cache->get(key);
            if (!marks)
                marks = load();
        }
    }
    else
        marks = load();

    if (!marks)
        throw Exception("Failed to load marks: " + mrk_path, ErrorCodes::LOGICAL_ERROR);   
}

}
