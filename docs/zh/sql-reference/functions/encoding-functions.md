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

接受`String`，`unsigned integer`，`Date`或`DateTime`类型的参数。返回包含参数的十六进制表示的字符串。使用大写字母`A-F`。不使用`0x`前缀或`h`后缀。对于字符串，所有字节都简单地编码为两个十六进制数字。数字转换为大端（«易阅读»）格式。对于数字，去除其中较旧的零，但仅限整个字节。例如，`hex（1）='01'`。 `Date`被编码为自Unix时间开始以来的天数。 `DateTime`编码为自Unix时间开始以来的秒数。

## unhex(str) {#unhexstr}

接受包含任意数量的十六进制数字的字符串，并返回包含相应字节的字符串。支持大写和小写字母A-F。十六进制数字的数量不必是偶数。如果是奇数，则最后一位数被解释为00-0F字节的低位。如果参数字符串包含除十六进制数字以外的任何内容，则返回一些实现定义的结果（不抛出异常）。
如果要将结果转换为数字，可以使用«reverse»和«reinterpretAsType»函数。

## UUIDStringToNum(str) {#uuidstringtonumstr}

接受包含36个字符的字符串，格式为«123e4567-e89b-12d3-a456-426655440000»，并将其转化为FixedString（16）返回。

## UUIDNumToString(str) {#uuidnumtostringstr}

接受FixedString（16）值。返回包含36个字符的文本格式的字符串。

## bitmaskToList(num) {#bitmasktolistnum}

接受一个整数。返回一个字符串，其中包含一组2的幂列表，其列表中的所有值相加等于这个整数。列表使用逗号分割，按升序排列。

## bitmaskToArray(num) {#bitmasktoarraynum}

接受一个整数。返回一个UInt64类型数组，其中包含一组2的幂列表，其列表中的所有值相加等于这个整数。数组中的数字按升序排列。

[来源文章](https://clickhouse.tech/docs/en/query_language/functions/encoding_functions/) <!--hide-->
