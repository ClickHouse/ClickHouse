SET(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/fleece)

add_library(fleece
${LIBRARY_DIR}/Fleece/Array.cc
${LIBRARY_DIR}/Fleece/Array.hh
${LIBRARY_DIR}/Fleece/Benchmark.hh
${LIBRARY_DIR}/Fleece/Dict.cc
${LIBRARY_DIR}/Fleece/Dict.hh
${LIBRARY_DIR}/Fleece/Encoder.cc
${LIBRARY_DIR}/Fleece/Encoder.hh
${LIBRARY_DIR}/Fleece/Endian.hh
${LIBRARY_DIR}/Fleece/Fleece+CoreFoundation.h
${LIBRARY_DIR}/Fleece/Fleece.h
${LIBRARY_DIR}/Fleece/Fleece.hh
${LIBRARY_DIR}/Fleece/Fleece.pch
${LIBRARY_DIR}/Fleece/Fleece_C_impl.cc
${LIBRARY_DIR}/Fleece/Fleece_C_impl.hh
${LIBRARY_DIR}/Fleece/FleeceCpp.hh
${LIBRARY_DIR}/Fleece/FleeceException.cc
${LIBRARY_DIR}/Fleece/FleeceException.hh
${LIBRARY_DIR}/Fleece/forestdb_endian.h
${LIBRARY_DIR}/Fleece/Internal.hh
${LIBRARY_DIR}/Fleece/JSON5.cc
${LIBRARY_DIR}/Fleece/JSON5.hh
${LIBRARY_DIR}/Fleece/JSONConverter.cc
${LIBRARY_DIR}/Fleece/JSONConverter.hh
${LIBRARY_DIR}/Fleece/JSONEncoder.cc
${LIBRARY_DIR}/Fleece/JSONEncoder.hh
${LIBRARY_DIR}/Fleece/KeyTree.cc
${LIBRARY_DIR}/Fleece/KeyTree.hh
${LIBRARY_DIR}/Fleece/Path.cc
${LIBRARY_DIR}/Fleece/Path.hh
${LIBRARY_DIR}/Fleece/PlatformCompat.hh
${LIBRARY_DIR}/Fleece/SharedKeys.cc
${LIBRARY_DIR}/Fleece/SharedKeys.hh
${LIBRARY_DIR}/Fleece/slice.cc
${LIBRARY_DIR}/Fleece/slice.hh
${LIBRARY_DIR}/Fleece/Stopwatch.hh
${LIBRARY_DIR}/Fleece/StringTable.cc
${LIBRARY_DIR}/Fleece/StringTable.hh
${LIBRARY_DIR}/Fleece/TempArray.hh
${LIBRARY_DIR}/Fleece/Value+Dump.cc
${LIBRARY_DIR}/Fleece/Value.cc
${LIBRARY_DIR}/Fleece/Value.hh
${LIBRARY_DIR}/Fleece/varint.cc
${LIBRARY_DIR}/Fleece/varint.hh
${LIBRARY_DIR}/Fleece/Writer.cc
${LIBRARY_DIR}/Fleece/Writer.hh
${LIBRARY_DIR}/Fleece/Mutable/MArray.hh
${LIBRARY_DIR}/Fleece/Mutable/MCollection.hh
${LIBRARY_DIR}/Fleece/Mutable/MContext.cc
${LIBRARY_DIR}/Fleece/Mutable/MContext.hh
${LIBRARY_DIR}/Fleece/Mutable/MDict.hh
${LIBRARY_DIR}/Fleece/Mutable/MDictIterator.hh
${LIBRARY_DIR}/Fleece/Mutable/MRoot.hh
${LIBRARY_DIR}/Fleece/Mutable/MValue.hh
${LIBRARY_DIR}/vendor/jsonsl/jsonsl.c
${LIBRARY_DIR}/vendor/libb64/cdecode.c
${LIBRARY_DIR}/vendor/libb64/cencode.c)

target_include_directories(fleece SYSTEM PUBLIC "${LIBRARY_DIR}/vendor/libb64")
target_include_directories(fleece SYSTEM PUBLIC "${LIBRARY_DIR}/vendor/jsonsl")

