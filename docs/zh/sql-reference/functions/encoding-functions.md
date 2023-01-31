# 编码函数 {#bian-ma-han-shu}

## char {#char}

返回长度为传递参数数量的字符串，并且每个字节都有对应参数的值。接受数字Numeric类型的多个参数。如果参数的值超出了UInt8数据类型的范围，则将其转换为UInt8，并可能进行舍入和溢出。

**语法**

``` sql
char(number_1, [number_2, ..., number_n]);
```

**参数**

-   `number_1, number_2, ..., number_n` — 数值参数解释为整数。类型: [Int](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md).

**返回值**

-   给定字节数的字符串。

类型: `String`。

**示例**

查询:

``` sql
SELECT char(104.1, 101, 108.9, 108.9, 111) AS hello
```

结果:

``` text
┌─hello─┐
│ hello │
└───────┘
```

你可以通过传递相应的字节来构造任意编码的字符串。 这是UTF-8的示例:

查询:

``` sql
SELECT char(0xD0, 0xBF, 0xD1, 0x80, 0xD0, 0xB8, 0xD0, 0xB2, 0xD0, 0xB5, 0xD1, 0x82) AS hello;
```

结果:

``` text
┌─hello──┐
│ привет │
└────────┘
```

查询:

``` sql
SELECT char(0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD) AS hello;
```

结果:

``` text
┌─hello─┐
│ 你好  │
└───────┘
```

## hex {#hex}

返回包含参数的十六进制表示的字符串。

别名为： `HEX`。

**语法**

``` sql
hex(arg)
```

该函数使用大写字母`A-F`，不使用任何前缀（如`0x`）或后缀（如`h`）

对于整数参数，它从高到低（大端或“人类可读”顺序）打印十六进制数字（“半字节”）。它从左侧第一个非零字节开始（省略前导零字节），但即使前导数字为零，也始终打印每个字节的两个数字。

类型为[Date](../../sql-reference/data-types/date.md)和[DateTime](../../sql-reference/data-types/datetime.md)的值将被格式化为相应的整数（日期为 Epoch 以来的天数，DateTime 为 Unix Timestamp 的值）。

对于[String](../../sql-reference/data-types/string.md)和[FixedString](../../sql-reference/data-types/fixedstring.md)，所有字节都被简单地编码为两个十六进制数字。零字节不会被省略。

类型为[Float](../../sql-reference/data-types/float.md)和[Decimal](../../sql-reference/data-types/decimal.md)的值被编码为它们在内存中的表示。由于我们支持小端架构，它们以小端编码。零前导尾随字节不会被省略。

类型为[UUID](../data-types/uuid.md)的值被编码为大端顺序字符串。

**参数**

-   `arg` — 要转换为十六进制的值。类型为[String](../../sql-reference/data-types/string.md)，[UInt](../../sql-reference/data-types/int-uint.md)，[Float](../../sql-reference/data-types/float.md)，[Decimal](../../sql-reference/data-types/decimal.md)，[Date](../../sql-reference/data-types/date.md)或者[DateTime](../../sql-reference/data-types/datetime.md)。

**返回值**

-   具有参数的十六进制表示的字符串。

类型为：[String](../../sql-reference/data-types/string.md)。

**示例**

查询语句：

``` sql
SELECT hex(1);
```

结果：

``` text
01
```

查询语句：

``` sql
SELECT hex(toFloat32(number)) AS hex_presentation FROM numbers(15, 2);
```

结果：

``` text
┌─hex_presentation─┐
│ 00007041         │
│ 00008041         │
└──────────────────┘
```

查询语句：

``` sql
SELECT hex(toFloat64(number)) AS hex_presentation FROM numbers(15, 2);
```

结果：

``` text
┌─hex_presentation─┐
│ 0000000000002E40 │
│ 0000000000003040 │
└──────────────────┘
```

查询语句：

``` sql
SELECT lower(hex(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'))) as uuid_hex
```

结果：

``` text
┌─uuid_hex─────────────────────────┐
│ 61f0c4045cb311e7907ba6006ad3dba0 │
└──────────────────────────────────┘
```

## unhex {#unhexstr}

执行[hex](#hex)函数的相反操作。它将每对十六进制数字（在参数中）解释为一个数字，并将其转换为该数字表示的字节。返回值是一个二进制字符串 (BLOB)。

如果要将结果转换为数字，可以使用 [reverse](../../sql-reference/functions/string-functions.md#reverse) 和 [reinterpretAs&lt;Type&gt;](../../sql-reference/functions/type-conversion-functions.md#type-conversion-functions) 函数。

:::注意    
如果从 `clickhouse-client` 中调用 `unhex`，二进制字符串将使用 UTF-8 显示。
:::

别名为：`UNHEX`。

**语法**

``` sql
unhex(arg)
```

**参数**

-   `arg` — 包含任意数量的十六进制数字的字符串。类型为：[String](../../sql-reference/data-types/string.md)。

支持大写和小写字母A-F。十六进制数字的数量不必是偶数。如果是奇数，则最后一位数被解释为00-0F字节的低位。如果参数字符串包含除十六进制数字以外的任何内容，则返回一些实现定义的结果（不抛出异常）。对于数字参数， unhex()不执行 hex(N) 的倒数。

**返回值**

-   二进制字符串 (BLOB)。

类型为： [String](../../sql-reference/data-types/string.md)。

**示例**

查询语句：
``` sql
SELECT unhex('303132'), UNHEX('4D7953514C');
```

结果：
``` text
┌─unhex('303132')─┬─unhex('4D7953514C')─┐
│ 012             │ MySQL               │
└─────────────────┴─────────────────────┘
```

查询语句：

``` sql
SELECT reinterpretAsUInt64(reverse(unhex('FFF'))) AS num;
```

结果：

``` text
┌──num─┐
│ 4095 │
└──────┘
```

## bin {#bin}

返回一个包含参数二进制表示的字符串。

**语法**

``` sql
bin(arg)
```

别名为： `BIN`。

对于整数参数，它从最高有效到最低有效（大端或“人类可读”顺序）打印 bin 数字。它从最重要的非零字节开始（省略前导零字节），但如果前导数字为零，则始终打印每个字节的八位数字。

类型为[Date](../../sql-reference/data-types/date.md)和[DateTime](../../sql-reference/data-types/datetime.md)的值被格式化为相应的整数（`Date` 为 Epoch 以来的天数，`DateTime` 为 Unix Timestamp 的值）。

对于[String](../../sql-reference/data-types/string.md)和[FixedString](../../sql-reference/data-types/fixedstring.md)，所有字节都被简单地编码为八个二进制数。零字节不会被省略。

类型为[Float](../../sql-reference/data-types/float.md)和[Decimal](../../sql-reference/data-types/decimal.md)的值被编码为它们在内存中的表示。由于我们支持小端架构，它们以小端编码。零前导尾随字节不会被省略。

类型为[UUID](../data-types/uuid.md)的值被编码为大端顺序字符串。

**参数**

-   `arg` — 要转换为二进制的值。类型为[String](../../sql-reference/data-types/string.md)，[FixedString](../../sql-reference/data-types/fixedstring.md)，[UInt](../../sql-reference/data-types/int-uint.md)，[Float](../../sql-reference/data-types/float.md)，[Decimal](../../sql-reference/data-types/decimal.md)，[Date](../../sql-reference/data-types/date.md)或者[DateTime](../../sql-reference/data-types/datetime.md)。

**返回值**

-   具有参数的二进制表示的字符串。

类型为： [String](../../sql-reference/data-types/string.md)。

**示例**

查询语句：

``` sql
SELECT bin(14);
```

结果：

``` text
┌─bin(14)──┐
│ 00001110 │
└──────────┘
```

查询语句：

``` sql
SELECT bin(toFloat32(number)) AS bin_presentation FROM numbers(15, 2);
```

结果：

``` text
┌─bin_presentation─────────────────┐
│ 00000000000000000111000001000001 │
│ 00000000000000001000000001000001 │
└──────────────────────────────────┘
```

查询语句：

``` sql
SELECT bin(toFloat64(number)) AS bin_presentation FROM numbers(15, 2);
```

结果：

``` text
┌─bin_presentation─────────────────────────────────────────────────┐
│ 0000000000000000000000000000000000000000000000000010111001000000 │
│ 0000000000000000000000000000000000000000000000000011000001000000 │
└──────────────────────────────────────────────────────────────────┘
```

查询语句：

``` sql
SELECT bin(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')) as bin_uuid
```

结果：

``` text
┌─bin_uuid─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ 01100001111100001100010000000100010111001011001100010001111001111001000001111011101001100000000001101010110100111101101110100000 │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```


## unbin {#unbinstr}

将每对二进制数字（在参数中）解释为一个数字，并将其转换为该数字表示的字节。这些函数执行与 [bin](#bin) 相反的操作。

**语法**

``` sql
unbin(arg)
```

别名为： `UNBIN`。

对于数字参数，`unbin()` 不会返回 `bin()` 的倒数。如果要将结果转换为数字，可以使用[reverse](../../sql-reference/functions/string-functions.md#reverse) 和 [reinterpretAs&lt;Type&gt;](../../sql-reference/functions/type-conversion-functions.md#reinterpretasuint8163264) 函数。

:::note    
如果从 `clickhouse-client` 中调用 `unbin`，则使用 UTF-8 显示二进制字符串。
:::

支持二进制数字`0`和`1`。二进制位数不必是八的倍数。如果参数字符串包含二进制数字以外的任何内容，则返回一些实现定义的结果（不抛出异常）。

**参数**

-   `arg` — 包含任意数量的二进制数字的字符串。类型为[String](../../sql-reference/data-types/string.md)。

**返回值**

-   二进制字符串 (BLOB)。

类型为：[String](../../sql-reference/data-types/string.md)。

**示例**

查询语句：

``` sql
SELECT UNBIN('001100000011000100110010'), UNBIN('0100110101111001010100110101000101001100');
```

结果：

``` text
┌─unbin('001100000011000100110010')─┬─unbin('0100110101111001010100110101000101001100')─┐
│ 012                               │ MySQL                                             │
└───────────────────────────────────┴───────────────────────────────────────────────────┘
```

查询语句：

``` sql
SELECT reinterpretAsUInt64(reverse(unbin('1110'))) AS num;
```

结果：

``` text
┌─num─┐
│  14 │
└─────┘
```

## UUIDStringToNum(str) {#uuidstringtonumstr}

接受包含36个字符的字符串，格式为«123e4567-e89b-12d3-a456-426655440000»，并将其转化为FixedString（16）返回。

## UUIDNumToString(str) {#uuidnumtostringstr}

接受FixedString（16）值。返回包含36个字符的文本格式的字符串。

## bitmaskToList(num) {#bitmasktolistnum}

接受一个整数。返回一个字符串，其中包含一组2的幂列表，其列表中的所有值相加等于这个整数。列表使用逗号分割，按升序排列。

## bitmaskToArray(num) {#bitmasktoarraynum}

接受一个整数。返回一个UInt64类型数组，其中包含一组2的幂列表，其列表中的所有值相加等于这个整数。数组中的数字按升序排列。

## bitPositionsToArray(num) {#bitpositionstoarraynum}

接受整数并将其转换为无符号整数。返回一个 `UInt64` 数字数组，其中包含 `arg` 中等于 `1` 的位的位置列表，按升序排列。

**语法**

```sql
bitPositionsToArray(arg)
```

**参数**

-   `arg` — 整数值。类型为[Int/UInt](../../sql-reference/data-types/int-uint.md)。

**返回值**

-   包含等于 `1` 的位位置列表的数组，按升序排列。

类型为： [Array](../../sql-reference/data-types/array.md)([UInt64](../../sql-reference/data-types/int-uint.md))。

**示例**

查询语句：

``` sql
SELECT bitPositionsToArray(toInt8(1)) AS bit_positions;
```

结果：

``` text
┌─bit_positions─┐
│ [0]           │
└───────────────┘
```

查询语句：

``` sql
SELECT bitPositionsToArray(toInt8(-1)) AS bit_positions;
```

结果：

``` text
┌─bit_positions─────┐
│ [0,1,2,3,4,5,6,7] │
└───────────────────┘
```


[来源文章](https://clickhouse.com/docs/en/query_language/functions/encoding_functions/) <!--hide-->
