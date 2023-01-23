#include <Dictionaries/RangeHashedDictionary.h>

/// RangeHashedDictionary is separated into two files
/// RangeHashedDictionary_simple.cpp and RangeHashedDictionary_complex.cpp 
/// to better parallelize the build procedure and avoid MSan build failure
/// due to excessive resource consumption.

namespace DB
{

template class RangeHashedDictionary<DictionaryKeyType::Simple>;

}
