#include <Common/StringSearcher.h>

namespace DB::impl
{

template class StringSearcher<true, true>;
template class StringSearcher<false, true>;
template class StringSearcher<true, false>;
template class StringSearcher<false, false>;
}
