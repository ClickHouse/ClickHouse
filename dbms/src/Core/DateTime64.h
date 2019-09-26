#pragma once

#include <Core/Types.h>

namespace DB {

//// this is a separate struct to avoid accidental conversions that
//// might occur between time_t and the type storing the datetime64
//// time_t might have a different definition on different libcs
//struct DateTime64 {
//    using Type = Int64;
//    struct Components {
//        time_t datetime = 0;
//        UInt32 nanos = 0;
//    };

//    Components split() const;
//    explicit DateTime64(Components c);
//    explicit DateTime64(Type tt) : t{tt} {}
//    explicit operator bool() const {
//        return t != 0;
//    }
//    Type get() const { return t; }
//private:
//    Type t;
//};

}
