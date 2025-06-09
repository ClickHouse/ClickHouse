#pragma once

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#pragma clang diagnostic ignored "-Wgnu-anonymous-struct"
#pragma clang diagnostic ignored "-Wnested-anon-types"
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wshadow-field-in-constructor"
#pragma clang diagnostic ignored "-Wdtor-name"
#if defined(__clang__) && __clang_major__ >= 21
#pragma clang diagnostic ignored "-Wms-bitfield-padding"
#endif
#include <re2/re2.h>
#include <re2/regexp.h>
#include <re2/walker-inl.h>
#pragma clang diagnostic pop
