#pragma once

#include <Core/Field.h>

namespace DB
{

/**
Binary encoding for Fields:
|--------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| Field type               | Binary encoding                                                                                                                |
|--------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| `Null`                   | `0x00`                                                                                                                         |
| `UInt64`                 | `0x01<var_uint_value>`                                                                                                         |
| `Int64`                  | `0x02<var_int_value>`                                                                                                          |
| `UInt128`                | `0x03<uint128_little_endian_value>`                                                                                            |
| `Int128`                 | `0x04<int128_little_endian_value>`                                                                                             |
| `UInt128`                | `0x05<uint128_little_endian_value>`                                                                                            |
| `Int128`                 | `0x06<int128_little_endian_value>`                                                                                             |
| `Float64`                | `0x07<float64_little_endian_value>`                                                                                            |
| `Decimal32`              | `0x08<var_uint_scale><int32_little_endian_value>`                                                                              |
| `Decimal64`              | `0x09<var_uint_scale><int64_little_endian_value>`                                                                              |
| `Decimal128`             | `0x0A<var_uint_scale><int128_little_endian_value>`                                                                             |
| `Decimal256`             | `0x0B<var_uint_scale><int256_little_endian_value>`                                                                             |
| `String`                 | `0x0C<var_uint_size><data>`                                                                                                    |
| `Array`                  | `0x0D<var_uint_size><value_encoding_1>...<value_encoding_N>`                                                                   |
| `Tuple`                  | `0x0E<var_uint_size><value_encoding_1>...<value_encoding_N>`                                                                   |
| `Map`                    | `0x0F<var_uint_size><key_encoding_1><value_encoding_1>...<key_endoding_N><value_encoding_N>`                                   |
| `IPv4`                   | `0x10<uint32_little_endian_value>`                                                                                             |
| `IPv6`                   | `0x11<uint128_little_endian_value>`                                                                                            |
| `UUID`                   | `0x12<uuid_value>`                                                                                                             |
| `Bool`                   | `0x13<bool_value>`                                                                                                             |
| `Object`                 | `0x14<var_uint_size><var_uint_key_size_1><key_data_1><value_encoding_1>...<var_uint_key_size_N><key_data_N><value_encoding_N>` |
| `AggregateFunctionState` | `0x15<var_uint_name_size><name_data><var_uint_data_size><data>`                                                                |
| `Negative infinity`      | `0xFE`                                                                                                                         |
| `Positive infinity`      | `0xFF`                                                                                                                         |
|--------------------------|--------------------------------------------------------------------------------------------------------------------------------|
*/

void encodeField(const Field &, WriteBuffer & buf);
Field decodeField(ReadBuffer & buf);

}
