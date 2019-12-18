#include <Storages/MarkCache.h>

namespace DB
{

class MergeTreeMarksLoader
{
public:
    using MarksPtr = MarkCache::MappedPtr;
    using LoadFunc = std::function<MarksPtr(const String &)>;

    MergeTreeMarksLoader() {}

    MergeTreeMarksLoader(MarkCache * mark_cache_,
        const String & mrk_path_,
        const LoadFunc & load_func_,
        bool save_marks_in_cache_,
        size_t columns_num_ = 1);

    const MarkInCompressedFile & getMark(size_t row_index, size_t column_index = 0);

    bool initialized() const { return marks != nullptr; }

private:
    MarkCache * mark_cache = nullptr;
    String mrk_path;
    LoadFunc load_func;
    bool save_marks_in_cache = false;
    size_t columns_num;
    MarksPtr marks;

    void loadMarks();
};

}
