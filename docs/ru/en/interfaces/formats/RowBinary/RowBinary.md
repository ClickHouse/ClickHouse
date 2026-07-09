---
alias: []
description: 'Документация по формату RowBinary'
input_format: true
keywords: ['RowBinary']
output_format: true
slug: /interfaces/formats/RowBinary
title: 'RowBinary'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Вход | Выход | Алиас |
| ---- | ----- | ----- |
| ✔    | ✔     |       |

<div id="description">
  ## Описание
</div>

Формат `RowBinary` разбирает данные построчно в бинарном формате.
Строки и значения записываются подряд, без разделителей.
Поскольку данные представлены в бинарном формате, разделитель после `FORMAT RowBinary` должен быть строго следующим:

* Любое количество пробельных символов:
  * `' '` (пробел — код `0x20`)
  * `'\t'` (табуляция — код `0x09`)
  * `'\f'` (перевод страницы — код `0x0C`)
* Затем ровно одна последовательность символов новой строки:
  * в стиле Windows `"\r\n"`
  * или в стиле Unix `'\n'`
* Сразу после неё следуют бинарные данные.

:::note
Этот формат менее эффективен, чем формат [Native](../Native.md), поскольку он является построчным.
:::

<div id="data-types-wire-format">
  ## Формат передачи типов данных
</div>

:::tip
Большинство запросов, приведённых в примерах, можно выполнить с помощью curl с выводом в файл.

```bash
curl -XPOST "http://localhost:8123?default_format=RowBinary" \
  --data-binary "SELECT 42 :: UInt32"  > out.bin
```

:::

Затем данные можно просмотреть в шестнадцатеричном редакторе.

<div id="unsigned-leb128">
  ### Беззнаковый LEB128 (Little Endian Base 128)
</div>

Это **беззнаковая** кодировка целых чисел переменной длины в формате **little-endian**, используемая для кодирования длины типов данных переменного размера, таких как `String`, `Array` и `Map`. Пример реализации можно найти на [странице LEB128 в Википедии](https://en.wikipedia.org/wiki/LEB128#Decode_unsigned_integer).

<div id="integer-types">
  ### (U)Int8, (U)Int16, (U)Int32, (U)Int64, (U)Int128, (U)Int256
</div>

Все целочисленные типы кодируются соответствующим количеством байтов в формате **little-endian**. Знаковые типы (`Int8`–`Int256`) используют представление в **дополнительном коде**. В большинстве языков такие целые числа можно извлекать из массивов байтов с помощью встроенных средств или широко известных библиотек. Для `Int128`/`Int256` и `UInt128`/`UInt256`, размер которых превышает встроенные целочисленные типы большинства языков, может потребоваться пользовательская десериализация.

<div id="bool">
  ### Bool
</div>

Булевы значения кодируются в один байт и могут быть десериализованы так же, как `UInt8`.

* `0` — это `false`
* `1` — это `true`

<div id="float32-float64">
  ### Float32, Float64
</div>

Числа с плавающей запятой в формате **little-endian**, закодированные в 4 байта для `Float32` и 8 байт для `Float64`. Как и для целых чисел, в большинстве языков есть подходящие средства для десериализации этих значений.

<div id="bfloat16">
  ### BFloat16
</div>

[BFloat16](https://clickhouse.com/docs/sql-reference/data-types/float#bfloat16) (Brain Floating Point) — это 16-битный формат чисел с плавающей точкой с диапазоном, как у Float32, но с меньшей точностью, что делает его полезным для задач машинного обучения. Формат передачи данных по сути представляет собой старшие 16 бит значения Float32. Если ваш язык не поддерживает его на нативном уровне, проще всего читать и записывать его как UInt16, преобразуя в Float32 и обратно:

Чтобы преобразовать BFloat16 в Float32 (псевдокод):

```text
// Read 2 bytes as little-endian UInt16
// Left-shift by 16 bits to get Float32 bits
bfloat16Bits = readUInt16()
float32Bits = bfloat16Bits << 16
floatValue = reinterpretAsFloat32(float32Bits)
```

Чтобы преобразовать Float32 в BFloat16 (псевдокод):

```text
// Right-shift Float32 bits by 16 to truncate to BFloat16
float32Bits = reinterpretAsUInt32(floatValue)
bfloat16Bits = float32Bits >> 16
writeUInt16(bfloat16Bits)
```

Примеры внутренних значений для `BFloat16`:

```sql
SELECT CAST(1.25, 'BFloat16')
```

```text
0xA0, 0x3F, // 1.25 as BFloat16
```

<div id="decimal">
  ### Decimal32, Decimal64, Decimal128, Decimal256
</div>

Типы Decimal представлены целыми числами в формате **little-endian** с соответствующей битовой разрядностью.

* `Decimal32` - 4 байта, или `Int32`.
* `Decimal64` - 8 байт, или `Int64`.
* `Decimal128` - 16 байт, или `Int128`.
* `Decimal256` - 32 байта, или `Int256`.

При десериализации значения Decimal целую и дробную части можно определить с помощью следующего псевдокода:

```text
let scale_multiplier = 10 ** scale
let whole_part = trunc(value / scale_multiplier)  // truncate toward zero
let fractional_part = value % scale_multiplier
let result = Decimal(whole_part, fractional_part)
```

Где `trunc` выполняет усечение к нулю (а не деление с округлением вниз, которое для отрицательных значений даёт другой результат), а `scale` — это количество цифр после десятичной точки. Например, для `Decimal(10, 2)` (эквивалент `Decimal32(2)`) значение `scale` равно `2`, а значение `12345` будет представлено как `(123, 45)`.

Для сериализации требуется выполнить обратную операцию:

```text
let scale_multiplier = 10 ** scale
let result = whole_part * scale_multiplier + fractional_part
```

Подробнее см. в [документации ClickHouse по типам Decimal](https://clickhouse.com/docs/sql-reference/data-types/decimal).

<div id="string">
  ### String
</div>

Строки в ClickHouse — это **произвольные последовательности байтов**. Они не обязаны быть корректной последовательностью UTF-8. Префикс длины — это **длина в байтах**, а не количество символов.

Кодируются в две части:

1. Целое число переменной длины (LEB128), указывающее длину строки в байтах.
2. Сами байты строки.

Например, строка `foobar` будет закодирована *семью* байтами следующим образом:

```text
0x06, // LEB128 length of the string (6)
0x66, // 'f'
0x6f, // 'o'
0x6f, // 'o'
0x62, // 'b'
0x61, // 'a'
0x72, // 'r'
```

<div id="fixedstring">
  ### FixedString
</div>

В отличие от `String`, `FixedString` имеет фиксированную длину, определяемую в схеме. Он кодируется как последовательность байтов и, если значение короче `N`, дополняется нулевыми байтами в конце.

:::note
При чтении `FixedString` нулевые байты в конце могут быть как дополнением, так и фактическими символами `\0` в данных — при передаче их невозможно различить. Сам ClickHouse сохраняет все `N` байт без изменений.
:::

Пустой `FixedString(3)` содержит только дополняющие нули:

```text
0x00, 0x00, 0x00
```

Непустая строка `FixedString(3)`, содержащая `hi`:

```text
0x68, // 'h'
0x69, // 'i'
0x00, // padding zero
```

Непустое значение `FixedString(3)`, содержащее строку `bar`:

```text
0x62, // 'b'
0x61, // 'a'
0x72, // 'r'
```

В последнем примере заполнение не требуется, поскольку используются все *три* байта.

<div id="date">
  ### Date
</div>

Хранится в виде `UInt16` (два байта), представляющего количество дней ***с*** `1970-01-01`.

Поддерживаемый диапазон значений: `[1970-01-01, 2149-06-06]`.

Примеры внутренних значений для `Date`:

```sql
SELECT CAST('2024-01-15', 'Date') AS d
```

```text
0x19, 0x4D, // 19737 as UInt16 (little-endian) = 19737 days since 1970-01-01
```

<div id="date32">
  ### Date32
</div>

Хранится как `Int32` (четыре байта), представляющее количество дней ***до или после*** `1970-01-01`.

Поддерживаемый диапазон значений: `[1900-01-01, 2299-12-31]`.

Примеры внутренних значений для `Date32`:

```sql
SELECT CAST('2024-01-15', 'Date32') AS d
```

```text
0x19, 0x4D, 0x00, 0x00, // 19737 as Int32 (little-endian) = 19737 days since 1970-01-01
```

Дата до начала эпохи:

```sql
SELECT CAST('1900-01-01', 'Date32') AS d
```

```text
0x21, 0x9C, 0xFF, 0xFF, // -25567 as Int32 (little-endian) = 25567 days before 1970-01-01
```

<div id="datetime">
  ### Дата и время
</div>

Хранится как `UInt32` (четыре байта), представляющее количество секунд ***с*** `1970-01-01 00:00:00 UTC`.

Синтаксис:

```text
DateTime([timezone])
```

Например, `DateTime` или `DateTime('UTC')`.

:::note
Бинарное значение всегда представляет собой смещение относительно эпохи UTC. Часовой пояс не влияет на кодирование. Однако часовой пояс **действительно** влияет на то, как строковые значения интерпретируются при вставке: при вставке `'2024-01-15 10:30:00'` в столбец `DateTime('America/New_York')` сохраняется другое значение эпохи, чем при вставке той же строки в столбец `DateTime('UTC')`, поскольку строка интерпретируется как местное время в часовом поясе столбца. On the wire в обоих случаях это просто секунды эпохи `UInt32`.
:::

Поддерживаемый диапазон значений: `[1970-01-01 00:00:00, 2106-02-07 06:28:15]`.

Примеры внутренних значений для `DateTime`:

```sql
SELECT CAST('2024-01-15 10:30:00', 'DateTime(\'UTC\')') AS d
```

```text
0x28, 0x09, 0xA5, 0x65, // 1705314600 as UInt32 (little-endian)
```

<div id="datetime64">
  ### DateTime64
</div>

Хранится как `Int64` (восемь байт), представляющее количество **тиков** ***до или после*** `1970-01-01 00:00:00 UTC`. Разрешение тика задаётся параметром `precision`, см. синтаксис ниже:

```text
DateTime64(precision, [timezone])
```

Где `precision` — целое число от `0` до `9`. Обычно используются только следующие значения: `3` (миллисекунды), `6` (микросекунды),
`9` (наносекунды).

Примеры допустимых определений DateTime64: `DateTime64(0)`, `DateTime64(3)`, `DateTime64(6, 'UTC')` или `DateTime64(9, 'Europe/Amsterdam')`.

:::note
Как и в случае с датой и временем, бинарное значение всегда является смещением относительно эпохи UTC. Часовой пояс влияет на то, как строковые значения интерпретируются при вставке (см. примечание [дата и время](#datetime)), но само представление всегда представляет собой `Int64` тиков с начала эпохи UTC.
:::

Внутреннее значение `Int64` типа `DateTime64` можно интерпретировать как количество следующих единиц времени до или после эпохи UNIX:

* `DateTime64(0)` - секунды.
* `DateTime64(3)` - миллисекунды.
* `DateTime64(6)` - микросекунды.
* `DateTime64(9)` - наносекунды.

Поддерживаемый диапазон значений: `[1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999]`.

Примеры внутренних значений для `DateTime64`:

* `DateTime64(3)`: значение `1546300800000` соответствует `2019-01-01 00:00:00 UTC`.
* `DateTime64(6)`: значение `1705314600123456` соответствует `2024-01-15 10:30:00.123456 UTC`.
* `DateTime64(9)`: значение `1705314600123456789` соответствует `2024-01-15 10:30:00.123456789 UTC`.

:::note
Точность максимального значения составляет 8 знаков. Если используется максимальная точность 9 знаков (наносекунды), максимально поддерживаемое значение — 2262-04-11 23:47:16 UTC.
:::

<div id="time">
  ### Time
</div>

Хранится в виде `Int32`, представляющего значение времени в секундах. Допускаются отрицательные значения.

Поддерживаемый диапазон значений: `[-999:59:59, 999:59:59]` (то есть `[-3599999, 3599999]` секунд).

:::note
В настоящее время для использования `Time` или `Time64` настройка `enable_time_time64_type` должна быть установлена в `1`.
:::

Примеры внутренних значений для `Time`:

```sql
SET enable_time_time64_type = 1;
SELECT CAST('15:32:16', 'Time') AS t
```

```text
0x80, 0xDA, 0x00, 0x00, // 55936 seconds = 15:32:16
```

<div id="time64">
  ### Time64
</div>

Внутренне хранится как `Decimal64` (который, в свою очередь, хранится как `Int64`) и представляет собой значение времени с дробной частью секунды и настраиваемой точностью. Отрицательные значения допустимы.

Синтаксис:

```text
Time64(precision)
```

Где `precision` — целое число от `0` до `9`. Наиболее распространённые значения: `3` (миллисекунды), `6` (микросекунды), `9` (наносекунды).

Поддерживаемый диапазон значений: `[-999:59:59.xxxxxxxxx, 999:59:59.xxxxxxxxx]`.

:::note
Сейчас для использования `Time` или `Time64` параметр `enable_time_time64_type` должен быть установлен в `1`.
:::

Внутреннее значение `Int64` представляет дробные секунды, масштабированные на `10^precision`.

Примеры внутренних значений для `Time64`:

```sql
SET enable_time_time64_type = 1;
SELECT CAST('15:32:16.123456', 'Time64(6)') AS t
```

```text
0x40, 0x82, 0x0D, 0x06,
0x0D, 0x00, 0x00, 0x00, // 55936123456 as Int64
// 55936123456 / 10^6 = 55936.123456 seconds = 15:32:16.123456
```

<div id="interval-types">
  ### Типы Interval
</div>

Все типы Interval хранятся как `Int64` (восемь байт, little-endian). Значение представляет собой количество соответствующих единиц времени. Отрицательные значения допустимы.

Типы Interval: `IntervalNanosecond`, `IntervalMicrosecond`, `IntervalMillisecond`, `IntervalSecond`, `IntervalMinute`, `IntervalHour`, `IntervalDay`, `IntervalWeek`, `IntervalMonth`, `IntervalQuarter`, `IntervalYear`.

:::note
Имя типа Interval (например, `IntervalSecond` или `IntervalDay`) определяет единицу измерения хранимого значения. wire encoding всегда одинаков.
:::

Примеры внутренних значений:

```sql
SELECT INTERVAL 5 SECOND   AS a,
     INTERVAL 10 DAY     AS b,
     INTERVAL -7 DAY     AS c,
     INTERVAL 3 YEAR     AS d,
     INTERVAL 500 MICROSECOND AS e
```

```text
// IntervalSecond: 5
0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
// IntervalDay: 10
0x0A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
// IntervalDay: -7
0xF9, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
// IntervalYear: 3
0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
// IntervalMicrosecond: 500
0xF4, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
```

<div id="enum8-enum16">
  ### Enum8, Enum16
</div>

Хранится как один байт (`Enum8` == `Int8`) или два байта (`Enum16` == `Int16`), которые представляют индекс значения enum в его определении. Обратите внимание, что тип хранения **знаковый** — значения enum могут быть отрицательными (например, `Enum8('a' = -128, 'b' = 0)`).

Enum можно определить простым способом, например так:

```sql
SELECT 1 :: Enum8('hello' = 1, 'world' = 2) AS e;
```

```text
   ┌─e─────┐
1. │ hello │
   └───────┘
```

Определённый выше Enum8 будет иметь на стороне клиента следующее отображение значений:

```text
Map<Int8, String> {
  1: 'hello',
  2: 'world'
}
```

Или, в более сложном варианте, вот так:

```sql
SELECT 42 :: Enum16('f\'' = 1, 'x =' = 2, 'b\'\'' = 3, '\'c=4=' = 42, '4' = 1234) AS e;
```

```text
   ┌─e─────┐
1. │ 'c=4= │
   └───────┘
```

Определённому выше `Enum16` на стороне клиента будет соответствовать следующее отображение значений:

```text
Map<Int16, String> {
  1:    'f\'',
  2:    'x =',
  3:    'b\'',
  42:   '\'c=4=',
  1234: '4'
}
```

Для парсера типов данных основная сложность — отслеживать экранированные символы в определении enum, например `\'`, а также специальные символы вроде `=`, которые могут встречаться внутри строк в кавычках.

<div id="uuid">
  ### UUID
</div>

Представляется последовательностью из 16 байт. UUID хранится как **два значения `UInt64` в формате little-endian**: первые 8 байт стандартного представления UUID записываются в обратном порядке, и вторые 8 байт также независимо записываются в обратном порядке.

Например, для UUID `61f0c404-5cb3-11e7-907b-a6006ad3dba0`:

* Стандартное байтовое представление: `61 f0 c4 04 5c b3 11 e7` | `90 7b a6 00 6a d3 db a0`
* Первая половина в обратном порядке (LE UInt64): `e7 11 b3 5c 04 c4 f0 61`
* Вторая половина в обратном порядке (LE UInt64): `a0 db d3 6a 00 a6 7b 90`

Примеры внутреннего значения для `UUID`:

* `61f0c404-5cb3-11e7-907b-a6006ad3dba0` представляется как:

```text
0xE7, 0x11, 0xB3, 0x5C, 0x04, 0xC4, 0xF0, 0x61,
0xA0, 0xDB, 0xD3, 0x6A, 0x00, 0xA6, 0x7B, 0x90,
```

* UUID по умолчанию `00000000-0000-0000-0000-000000000000` представлен в виде 16 нулевых байтов:

```text
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
```

Его можно использовать, если была вставлена новая запись, но значение UUID не было указано.

<div id="ipv4">
  ### IPv4
</div>

Хранится в четырёх байтах как `UInt32` с порядком байтов **little-endian**. Обратите внимание, что это отличается от традиционного сетевого порядка байтов (big-endian), обычно используемого для IP-адресов. Примеры внутренних значений для `IPv4`:

```sql
SELECT    
  CAST('0.0.0.0',         'IPv4') AS a,
  CAST('127.0.0.1',       'IPv4') AS b,
  CAST('192.168.0.1',     'IPv4') AS c,
  CAST('255.255.255.255', 'IPv4') AS d,
  CAST('168.212.226.204', 'IPv4') AS e
```

```text
0x00, 0x00, 0x00, 0x00, // 0.0.0.0
0x01, 0x00, 0x00, 0x7f, // 127.0.0.1
0x01, 0x00, 0xa8, 0xc0, // 192.168.0.1
0xff, 0xff, 0xff, 0xff, // 255.255.255.255
0xcc, 0xe2, 0xd4, 0xa8, // 168.212.226.204
```

<div id="ipv6">
  ### IPv6
</div>

Хранится в 16 байтах в **порядке big-endian / сетевом порядке байтов** (старший байт первым). Примеры внутренних значений для `IPv6`:

```sql
SELECT
    CAST('2a02:aa08:e000:3100::2',        'IPv6') AS a,
    CAST('2001:44c8:129:2632:33:0:252:2', 'IPv6') AS b,
    CAST('2a02:e980:1e::1',               'IPv6') AS c
```

```text
// 2a02:aa08:e000:3100::2
0x2A, 0x02, 0xAA, 0x08, 0xE0, 0x00, 0x31, 0x00, 
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
// 2001:44c8:129:2632:33:0:252:2
0x20, 0x01, 0x44, 0xC8, 0x01, 0x29, 0x26, 0x32, 
0x00, 0x33, 0x00, 0x00, 0x02, 0x52, 0x00, 0x02,
// 2a02:e980:1e::1
0x2A, 0x02, 0xE9, 0x80, 0x00, 0x1E, 0x00, 0x00, 
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
```

<div id="nullable">
  ### Nullable
</div>

Тип данных, допускающий `NULL`, кодируется следующим образом:

1. Один байт, указывающий, является ли значение `NULL` или нет:
   * `0x00` означает, что значение не равно `NULL`.
   * `0x01` означает, что значение равно `NULL`.
2. Если значение не `NULL`, базовый тип данных кодируется как обычно. Если значение `NULL`, для базового типа **не записываются никакие дополнительные байты**.

Например, значение `Nullable(UInt32)`:

```sql
SELECT    
   CAST(42,   'Nullable(UInt32)') AS a,
   CAST(NULL, 'Nullable(UInt32)') AS b
```

```text
0x00,                   // Not NULL - the value follows
0x2A, 0x00, 0x00, 0x00, // UInt32(42)
0x01,                   // NULL - nothing follows
```

<div id="lowcardinality">
  ### LowCardinality
</div>

В формате RowBinary маркер low-cardinality не влияет на формат передачи данных. Например, `LowCardinality(String)` кодируется так же, как обычный `String`.

:::warning
Это относится только к RowBinary. В формате Native `LowCardinality` использует другое кодирование на основе словаря.
:::

:::note
Столбец можно определить как `LowCardinality(Nullable(T))`, но его нельзя определить как `Nullable(LowCardinality(T))` — это всегда будет приводить к ошибке сервера.
:::

При тестировании для [allow&#95;suspicious&#95;low&#95;cardinality&#95;types](https://clickhouse.com/docs/operations/settings/settings#allow_suspicious_low_cardinality_types) можно установить значение `1`, чтобы разрешить большинство типов данных внутри `LowCardinality` и обеспечить более полное покрытие.

<div id="array">
  ### Array
</div>

Массив кодируется следующим образом:

1. [Целое число переменной длины (LEB128)](#unsigned-leb128), которое указывает количество элементов в массиве.
2. Элементы массива, закодированные так же, как и базовый тип данных.

Например, массив со значениями `UInt32`:

```sql
SELECT CAST(array(1, 2, 3), 'Array(UInt32)') AS arr
```

```text
0x03,                   // LEB128 - the array has 3 elements
0x01, 0x00, 0x00, 0x00, // UInt32(1)
0x02, 0x00, 0x00, 0x00, // UInt32(2)
0x03, 0x00, 0x00, 0x00, // UInt32(3)
```

Чуть более сложный пример:

```sql
SELECT array('foobar', 'qaz') AS arr
```

```text
0x02,             // LEB128 - the array has 2 elements
0x06,             // LEB128 - the first string has 6 bytes
0x66, 0x6f, 0x6f, 
0x62, 0x61, 0x72, // 'foobar'
0x03,             // LEB128 - the second string has 3 bytes
0x71, 0x61, 0x7a, // 'qaz'
```

:::note
Массив может содержать значения Nullable, но сам массив не может быть Nullable.
:::

Допустим следующий вариант:

```sql
SELECT CAST([NULL, 'foo'], 'Array(Nullable(String))') AS arr;
```

```text
   ┌─arr──────────┐
1. │ [NULL,'foo'] │
   └──────────────┘
```

И он будет кодироваться следующим образом:

```text
0x02,             // LEB128  - the array has 2 elements
0x01,             // Is NULL - nothing follows for this element
0x00,             // Is NOT NULL - the data follows
0x03,             // LEB128  - the string has 3 bytes
0x66, 0x6f, 0x6f, // 'foo'
```

Пример работы с многомерными массивами приведён в [разделе Geo](#geo-types).

<div id="tuple">
  ### Tuple
</div>

Кортеж кодируется как последовательность его элементов, следующих друг за другом в соответствующем формате передачи данных, без какой-либо дополнительной метаинформации или разделителей.

```sql
CREATE OR REPLACE TABLE foo
(
    `t` Tuple(
           UInt32,
           String,
           Array(UInt8)
        )
)
ENGINE = Memory;
INSERT INTO foo VALUES ((42, 'foo', array(99, 144)));
```

```text
0x2a, 0x00, 0x00, 0x00, // 42 as UInt32
0x03,                   // LEB128 - the string has 3 bytes
0x66, 0x6f, 0x6f,       // 'foo'
0x02,                   // LEB128 - the array has 2 elements
0x63,                   // 99 as UInt8
0x90,                   // 144 as UInt8
```

Строковое кодирование типа данных Tuple сопряжено с теми же сложностями, что и [тип Enum](#enum8-enum16): например, необходимо отслеживать экранированные символы и специальные знаки; кроме того, в случае с Tuple также требуется отслеживать открывающие и закрывающие круглые скобки. Кроме того, обратите внимание, что наиболее сложные Tuple могут содержать другие вложенные Tuple, Arrays, Maps и даже enum.

Например, в следующей таблице tuple содержит enum с символом tick и круглой скобкой в имени, что может вызвать проблемы при parsing, если это обрабатывать неправильно:

```sql
CREATE OR REPLACE TABLE foo
(
   `t` Tuple(
          Enum8('f\'()' = 0),
          Array(Nullable(Tuple(UInt32, String)))
       )
) ENGINE = Memory;
```

<div id="map">
  ### Map
</div>

Map можно представить как `Array(Tuple(K, V))`, где `K` — тип ключа, а `V` — тип значения. Map кодируется следующим образом:

1. [Целое число переменной длины (LEB128)](#unsigned-leb128), указывающее количество элементов в Map.
2. Элементы Map в виде пар ключ-значение, закодированных в соответствии с их типами.

Например, Map с ключами типа `String` и значениями типа `UInt32`:

```sql
SELECT CAST(map('foo', 1, 'bar', 2), 'Map(String, UInt32)') AS m
```

```text
0x02,                   // LEB128 - the map has 2 elements
0x03,                   // LEB128 - the first key has 3 bytes
0x66, 0x6f, 0x6f,       // 'foo'
0x01, 0x00, 0x00, 0x00, // UInt32(1)
0x03,                   // LEB128 - the second key has 3 bytes
0x62, 0x61, 0x72,       // 'bar'
0x02, 0x00, 0x00, 0x00, // UInt32(2)
```

:::note
Возможны значения типа Map с глубоко вложенными структурами, например `Map(String, Map(Int32, Array(Nullable(String))))`; они будут кодироваться аналогично описанному выше.
:::

<div id="variant">
  ### Variant
</div>

Этот тип представляет собой объединение других типов данных. Тип `Variant(T1, T2, ..., TN)` означает, что в каждой строке этого типа может находиться значение либо типа `T1`, либо `T2`, либо …, либо `TN`, либо ни одного из них (значение `NULL`).

:::warning
Хотя для конечного пользователя `Variant(T1, T2)` означает ровно то же самое, что и `Variant(T2, T1)`, порядок типов в определении важен для формата передачи данных: типы в определении всегда сортируются по алфавиту, и это важно, поскольку конкретный вариант кодируется с помощью &quot;дискриминанта&quot; — индекса типа данных в определении.
:::

Рассмотрим следующий пример:

```sql
SET allow_experimental_variant_type = 1,
    allow_suspicious_variant_types = 1;
CREATE OR REPLACE TABLE foo
(
  -- It does not matter what is the order of types in the user input;
  -- the types are always sorted alphabetically in the wire format.
  `var` Variant(
           Array(Int16),
           Bool,
           Date,
           FixedString(6),
           Float32, Float64,
           Int128, Int16, Int32, Int64, Int8,
           String,
           UInt128, UInt16, UInt32, UInt64, UInt8
       )
)
ENGINE = MergeTree
ORDER BY ();
INSERT INTO foo VALUES (true), ('foobar' :: FixedString(6)), (100.5 :: Float64), (100 :: Int128), ([1, 2, 3] :: Array(Int16));
SELECT * FROM foo FORMAT RowBinary;
```

```text
0x01,                               // type index -> Bool
 0x01,                               // true
 0x03,                               // type index -> FixedString(6)
 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, // 'foobar' 
 0x05,                               // type index -> Float64
 0x00, 0x00, 0x00, 0x00, 
 0x00, 0x20, 0x59, 0x40,             // 100.5 as Float64
 0x06,                               // type index -> Int128
 0x64, 0x00, 0x00, 0x00, 
 0x00, 0x00, 0x00, 0x00, 
 0x00, 0x00, 0x00, 0x00, 
 0x00, 0x00, 0x00, 0x00,             // 100 as Int128
 0x00,                               // type index -> Array(Int16)
 0x03,                               // LEB128 - the array has 3 elements
 0x01, 0x00,                         // 1 as Int16
 0x02, 0x00,                         // 2 as Int16
 0x03, 0x00,                         // 3 as Int16
```

Значение `NULL` кодируется байтом-дискриминантом `0xFF`:

```sql
SELECT NULL :: Variant(UInt32, String)
```

```text
0xFF, // discriminant = NULL
```

Параметр [allow&#95;suspicious&#95;variant&#95;types](https://clickhouse.com/docs/operations/settings/settings#allow_suspicious_variant_types) можно использовать для более полного тестирования типа `Variant`.

<div id="dynamic">
  ### Dynamic
</div>

Тип `Dynamic` может содержать значения любого типа, определяемого во время выполнения. В формате RowBinary каждое значение является самоописывающимся: первая часть — это спецификация типа в [таком формате](https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding). Далее следует содержимое, а кодировка значения описана в этом документе. Поэтому, чтобы разобрать значение, достаточно использовать индекс типа для выбора подходящего парсера, а затем повторно использовать уже имеющийся у вас код разбора RowBinary.

```text
[BinaryTypeIndex][type-specific parameters...][value]
```

Где `BinaryTypeIndex` — это один байт, идентифицирующий тип. Индексы типов и параметры см. в справочнике [здесь](https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding).

Значение `NULL` типа Dynamic кодируется с `BinaryTypeIndex` `0x00` (тип `Nothing`) без дополнительных байтов:

```sql
SELECT NULL::Dynamic
```

```text
00                        # BinaryTypeIndex: Nothing (0x00), represents NULL
```

**Примеры:**

```sql
SELECT 42::Dynamic
```

```text
0a                        # BinaryTypeIndex: Int64 (0x0A)
2a 00 00 00 00 00 00 00   # Int64 value: 42
```

```sql
SELECT toDateTime64('2024-01-15 10:30:00', 3, 'America/New_York')::Dynamic
```

```text
14                        # BinaryTypeIndex: DateTime64WithTimezone (0x14)
03                        # UInt8: precision
10                        # VarUInt: timezone name length
41 6d 65 72 69 63 61 2f   # "America/"
4e 65 77 5f 59 6f 72 6b   # "New_York"
c0 6c be 0d 8d 01 00 00   # Int64: timestamps
```

<div id="json">
  ### JSON
</div>

Тип JSON кодирует данные в двух различных категориях:

1. **Типизированные пути** - Пути, объявленные в схеме с явным указанием типов (например, `JSON(user_id UInt32, name String)`)
2. **Динамические пути/пути переполнения при превышении лимита динамических путей** - Пути, обнаруженные во время выполнения и сохраняемые как тип `Dynamic`. Кодированию значения предшествует определение типа.

Формат передачи данных и правила для этих двух категорий различаются.

| Категория пути          | Включается в сериализацию   | Кодирование значения          | Допускаются Variant/Nullable |
| ----------------------- | --------------------------- | ----------------------------- | ---------------------------- |
| **Типизированные пути** | Всегда (даже при NULL)      | Типозависимый бинарный формат | Да                           |
| **Динамические пути**   | Только для не-NULL значений | Dynamic                       | Нет                          |

Пути сериализуются в трёх группах, записываемых последовательно: типизированные пути, динамические пути, затем пути общих данных (overflow). Типизированные и динамические пути записываются в порядке, определяемом реализацией (определяется порядком итерации по внутренней хеш-таблице), тогда как пути общих данных записываются в алфавитном порядке. Не следует полагаться на какой-либо конкретный порядок путей. Десериализатор обрабатывает каждый путь по имени, а не по позиции.

Каждая JSON-строка в формате RowBinary сериализуется следующим образом:

```text
[VarUInt: number_of_paths]
[String: path_1][value_1]
[String: path_2][value_2]
...
```

**Примеры:**

**1. Простой JSON только с типизированными путями:**

Схема: `JSON(user_id UInt32, active Bool)`

Строка: `{"user_id": 42, "active": true}`

Двоичное кодирование (hex с аннотациями):

```text
02                              # VarUInt: 2 paths total

# Typed path "active"
06 61 63 74 69 76 65            # String: "active" (length 6 + bytes)
01                              # Bool/UInt8 value: true (1)

# Typed path "user_id"
07 75 73 65 72 5F 69 64         # String: "user_id" (length 7 + bytes)
2A 00 00 00                     # UInt32 value: 42 (little-endian)
```

**2. Простой JSON с типизированными и динамическими путями:**

Схема: `JSON(user_id UInt32, active Bool)`

Строка: `{"user_id": 42, "active": true, "name": "Alice"}`

Двоичное кодирование (hex с комментариями):

```text
03                              # VarUInt: 3 paths total

# Typed path "active"
06 61 63 74 69 76 65            # String: "active" (length 6 + bytes)
01                              # Bool/UInt8 value: true (1)

# Dynamic path "name"
04 6E 61 6D 65                  # String: "name" (length 4 + bytes)
15                              # BinaryTypeIndex: String (0x15)
05 41 6C 69 63 65               # String value: "Alice" (length 5 + bytes)

# Typed path "user_id"
07 75 73 65 72 5F 69 64         # String: "user_id" (length 7 + bytes)
2A 00 00 00                     # UInt32 value: 42 (little-endian)

```

**3. Обработка значений NULL:**

Для типизированного Nullable-столбца вы получите null:

Схема: `JSON(score Nullable(Int32))`

Строка: `{"score": null }`

Двоичное кодирование (hex с аннотациями):

```text
01                              # VarUInt: 1 path total

# Typed path "score" (Nullable)
05 73 63 6f 72 65               # String: "score" (length 5 + bytes)
01                              # Nullable flag: 1 (is NULL, no value follows)
```

Для типизированного non-NULL столбца возвращается значение по умолчанию:

Схема: `JSON(name String)`

Строка: `{"name": null}`

Двоичное кодирование:

```text
01                              # VarUInt: 1 path (dynamic NULL paths are skipped!)

04 6e 61 6d 65  # "name"
00              # String length 0 (empty string)
```

При динамическом пути он игнорируется:

Схема: `JSON(id UInt64)`

Строка: `{"id": 100, "metadata": null}`

Двоичное кодирование:

```text
01                              # VarUInt: 1 path (dynamic NULL paths are skipped!)

# Typed path "id"
02 69 64                        # String: "id" (length 2 + bytes)
64 00 00 00 00 00 00 00         # UInt64 value: 100 (little-endian)

```

Note: Путь `metadata` со значением NULL **не включается**, так как динамические пути сериализуются только при ненулевых значениях. Это ключевая разность по сравнению с типизированными путями.

**4. Вложенные JSON-объекты:**

Схема: `JSON()`

Строка: `{"user": {"name": "Bob", "age": 30}}`

Двоичное кодирование (hex, с аннотациями):

```text
02                              # VarUInt: 2 paths (nested objects are flattened)

# Dynamic path "user.age"
08 75 73 65 72 2E 61 67 65      # String: "user.age" (length 8 + bytes)
0A                              # BinaryTypeIndex: Int64 (0x0A)
1E 00 00 00 00 00 00 00         # Int64 value: 30 (little-endian)

# Dynamic path "user.name"
09 75 73 65 72 2E 6E 61 6D 65   # String: "user.name" (length 9 + bytes)
15                              # BinaryTypeIndex: String (0x15)
03 42 6F 62                     # String value: "Bob" (length 3 + bytes)

```

Примечание: объекты Nested преобразуются в плоскую структуру с путями, разделёнными точками (например, `user.name` вместо вложенной структуры).

**Альтернатива: режим «JSON как строка»**

При настройке `output_format_binary_write_json_as_string=1` JSON-столбцы сериализуются как одна строка текста JSON, а не в структурированном бинарном формате. Для записи в JSON-столбцы есть соответствующая настройка — `input_format_binary_read_json_as_string`. Выбор настройки здесь сводится к тому, хотите ли вы разбирать JSON на стороне клиента или сервера.

<div id="geo-types">
  ### Гео-типы
</div>

Geo — это категория типов данных для представления географических данных. Она включает:

* `Point` - в виде `Tuple(Float64, Float64)`.
* `Ring` - в виде `Array(Point)` или `Array(Tuple(Float64, Float64))`.
* `Polygon` - в виде `Array(Ring)` или `Array(Array(Tuple(Float64, Float64)))`.
* `MultiPolygon` - в виде `Array(Polygon)` или `Array(Array(Array(Tuple(Float64, Float64))))`.
* `LineString` - в виде `Array(Point)` или `Array(Tuple(Float64, Float64))`.
* `MultiLineString` - в виде `Array(LineString)` или `Array(Array(Tuple(Float64, Float64)))`.

Формат передачи данных для значений Geo точно такой же, как у Tuple и Array. Заголовки формата `RowBinaryWithNamesAndTypes` содержат псевдонимы этих типов, например `Point`, `Ring`, `Polygon`, `MultiPolygon`, `LineString` и `MultiLineString`.

```sql
SELECT    (1.0, 2.0)                                       :: Point           AS point,
    [(3.0, 4.0), (5.0, 6.0)]                         :: Ring            AS ring,
    [[(7.0, 8.0), (9.0, 10.0)], [(11.0, 12.0)]]      :: Polygon         AS polygon,
    [[[(13.0, 14.0), (15.0, 16.0)], [(17.0, 18.0)]]] :: MultiPolygon    AS multi_polygon,
    [(19.0, 20.0), (21.0, 22.0)]                     :: LineString      AS line_string,
    [[(23.0, 24.0), (25.0, 26.0)], [(27.0, 28.0)]]   :: MultiLineString AS multi_line_string
```

```text
// Point - or Tuple(Float64, Float64)
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // Point.X
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // Point.Y
// Ring - or Array(Tuple(Float64, Float64))
0x02, // LEB128 - the "ring" array has 2 points
   // Ring - Point #1
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, 
   // Ring - Point #2
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, 
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, 
// Polygon - or Array(Array(Tuple(Float64, Float64)))
0x02, // LEB128 - the "polygon" array has 2 rings
   0x02, // LEB128 - the first ring has 2 points
      // Polygon - Ring #1 - Point #1
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1C, 0x40, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x40,
      // Polygon - Ring #1 - Point #2
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x40, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, 
  0x01, // LEB128 - the second ring has 1 point
      // Polygon - Ring #2 - Point #1 (the only one)
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x26, 0x40, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x28, 0x40, 
// MultiPolygon - or Array(Array(Array(Tuple(Float64, Float64))))
0x01, // LEB128 - the "multi_polygon" array has 1 polygon
   0x02, // LEB128 - the first polygon has 2 rings
      0x02, // LEB128 - the first ring has 2 points
         // MultiPolygon - Polygon #1 - Ring #1 - Point #1
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x40, 
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2C, 0x40,
         // MultiPolygon - Polygon #1 - Ring #1 - Point #2
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2E, 0x40, 
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x40, 
      0x01, // LEB128 - the second ring has 1 point
        // MultiPolygon - Polygon #1 - Ring #2 - Point #1 (the only one)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x31, 0x40, 
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, 0x40, 
 // LineString - or Array(Tuple(Float64, Float64))
 0x02, // LEB128 - the line string has 2 points
    // LineString - Point #1
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x40, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40,
    // LineString - Point #2
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x35, 0x40, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x36, 0x40, 
 // MultiLineString - or Array(Array(Tuple(Float64, Float64)))
 0x02, // LEB128 - the multi line string has 2 line strings
   0x02, // LEB128 - the first line string has 2 points
     // MultiLineString - LineString #1 - Point #1
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x37, 0x40, 
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x38, 0x40, 
     // MultiLineString - LineString #1 - Point #2
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x39, 0x40, 
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3A, 0x40, 
   0x01, // LEB128 - the second line string has 1 point
     // MultiLineString - LineString #2 - Point #1 (the only one)
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3B, 0x40, 
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3C, 0x40,
```

<div id="geometry">
  ### Geometry
</div>

`Geometry` — это тип `Variant`, который может содержать любой из перечисленных выше гео-типов. В формате передачи данных он кодируется точно так же, как `Variant`: с байтом-дискриминантом, который указывает, какой гео-тип идет следующим.

Индексы дискриминантов для Geometry:

| Index | Type            |
| ----- | --------------- |
| 0     | LineString      |
| 1     | MultiLineString |
| 2     | MultiPolygon    |
| 3     | Point           |
| 4     | Polygon         |
| 5     | Ring            |

Структура формата передачи данных:

```text
// 1 byte discriminant (0-5)
// followed by the corresponding geo type data
```

Пример кодирования `Point` в виде `Geometry`:

```sql
SELECT ((1.0, 2.0)::Point)::Geometry
```

```text
0x03,                                           // discriminant = 3 (Point)
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // Point.X = 1.0 as Float64
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // Point.Y = 2.0 as Float64
```

Пример представления `Ring` в виде `Geometry`:

```text
0x05,       // discriminant = 5 (Ring)
0x02,       // LEB128 - array has 2 points
// Point #1
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // X = 3.0
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, // Y = 4.0
// Point #2
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, // X = 5.0
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, // Y = 6.0
```

<div id="nested">
  ### Nested
</div>

Формат передачи данных для `Nested` зависит от настройки `flatten_nested`.

:::warning
Все массивы компонентов в одной строке **должны иметь одинаковую длину**. Это ограничение контролируется сервером. Несовпадение длин приведёт к ошибкам вставки.
:::

<div id="nested-flattened">
  #### `flatten_nested = 1` (по умолчанию)
</div>

При значении по умолчанию `Nested` разворачивается в независимые массивы. Каждый подстолбец становится отдельным столбцом `Array` с именем, части которого разделены точками:

```sql
CREATE OR REPLACE TABLE foo
(
    n Nested(a String, b Int32)
) ENGINE = MergeTree ORDER BY ();
-- flatten_nested=1 is the default
INSERT INTO foo VALUES (['foo', 'bar'], [42, 144]);
```

`DESCRIBE TABLE foo` показывает плоские столбцы:

```text
   ┌─name─┬─type──────────┐
1. │ n.a  │ Array(String) │
2. │ n.b  │ Array(Int32)  │
   └──────┴───────────────┘
```

Каждый массив сериализуется отдельно, как описано в разделе [Array](#array):

```text
0x02,                   // LEB128 - 2 String elements in the first array (n.a)
 0x03,                   // LEB128 - the first string has 3 bytes
 0x66, 0x6F, 0x6F,       // 'foo'
 0x03,                   // LEB128 - the second string has 3 bytes
 0x62, 0x61, 0x72,       // 'bar'
0x02,                   // LEB128 - 2 Int32 elements in the second array (n.b)
 0x2A, 0x00, 0x00, 0x00, // 42 as Int32
 0x90, 0x00, 0x00, 0x00, // 144 as Int32
```

<div id="nested-unflattened">
  #### `flatten_nested = 0`
</div>

При `flatten_nested = 0` тип `Nested` сохраняется как один столбец типа `Array(Tuple(...))`. Имя столбца не содержит разделения точками:

```sql
SET flatten_nested = 0;
CREATE OR REPLACE TABLE foo
(
    n Nested(a String, b Int32)
) ENGINE = MergeTree ORDER BY ();
INSERT INTO foo VALUES ([('foo', 42), ('bar', 144)]);
```

`DESCRIBE TABLE foo` возвращает один столбец:

```text
   ┌─name─┬─type───────────────────────┐
1. │ n    │ Nested(a String, b Int32)  │
   └──────┴────────────────────────────┘
```

Кодировка — `Array(Tuple(String, Int32))`: сначала префикс длины массива, затем поля Tuple каждого элемента по порядку:

```text
0x02,                   // LEB128 - 2 elements in the array
 0x03,                   // LEB128 - first tuple, field a: 3 bytes
 0x66, 0x6F, 0x6F,       // 'foo'
 0x2A, 0x00, 0x00, 0x00, // first tuple, field b: 42 as Int32
 0x03,                   // LEB128 - second tuple, field a: 3 bytes
 0x62, 0x61, 0x72,       // 'bar'
 0x90, 0x00, 0x00, 0x00, // second tuple, field b: 144 as Int32
```

Обратите внимание, что поля чередуются по элементам (a₁, b₁, a₂, b₂), а не группируются по столбцам (a₁, a₂, b₁, b₂), как в плоском представлении.

<div id="simpleaggregatefunction">
  ### SimpleAggregateFunction
</div>

`SimpleAggregateFunction(func, T)` кодируется так же, как базовый тип данных `T`. Имя агрегатной функции не влияет на формат передачи данных.

Например, `SimpleAggregateFunction(max, UInt32)` кодируется так же, как обычный `UInt32`:

```sql
CREATE TABLE test_saf
(
    key UInt32,
    val SimpleAggregateFunction(max, UInt32)
) ENGINE = AggregatingMergeTree ORDER BY key;

INSERT INTO test_saf VALUES (1, 42);
SELECT val FROM test_saf;
```

В заголовке RowBinaryWithNamesAndTypes тип указан как `SimpleAggregateFunction(max, UInt32)`, а в передаваемых данных значение — просто `UInt32`:

```text
0x2A, 0x00, 0x00, 0x00, // 42 as UInt32
```

<div id="aggregatefunction">
  ### AggregateFunction
</div>

`AggregateFunction(func, T)` хранит полное промежуточное состояние агрегатной функции. В отличие от `SimpleAggregateFunction`, которая тоже хранит промежуточное состояние, но кодирует его так же, как и базовый тип данных, `AggregateFunction` хранит непрозрачный бинарный блок данных, формат которого зависит от конкретной агрегатной функции.

:::warning
Агрегатные состояния **не имеют префикса длины** в RowBinary. Парсер должен понимать внутренний формат сериализации каждой конкретной агрегатной функции, чтобы знать, сколько байтов нужно прочитать. На практике большинство клиентов рассматривают агрегатные состояния как непрозрачные и используют комбинаторы `*State` / `*Merge`, чтобы сервер сам выполнял сериализацию.
:::

Внутренний формат зависит от функции. Вот несколько простых примеров:

**`countState`** — хранит количество как VarUInt (LEB128):

```sql
SELECT countState(number) FROM numbers(5)
```

```text
0x05, // VarUInt: 5
```

**`sumState`** — сохраняет накопленную сумму в целочисленном типе фиксированного размера. Разрядность зависит от типа аргумента (`UInt64` для целочисленных аргументов):

```sql
SELECT sumState(toUInt32(number)) FROM numbers(5) -- sum = 0+1+2+3+4 = 10
```

```text
0x0A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 10 as UInt64
```

**`minState` / `maxState`** — хранит байт-флаг, за которым следует значение базового типа. Флаг принимает значение `0x00` для пустого состояния (значение ещё не встречалось) или `0x01`, если значение присутствует:

```sql
SELECT maxState(toUInt32(number)) FROM numbers(5) -- max = 4
```

```text
0x01,                   // flag: has value
0x04, 0x00, 0x00, 0x00, // 4 as UInt32
```

Пустое состояние (ни одна строка не агрегирована):

```sql
SELECT minState(toUInt32(number)) FROM numbers(0)
```

```text
0x00, // flag: no value
```

:::note
Более сложные функции, такие как `uniq`, `quantile` и `groupArray`, используют форматы, зависящие от реализации. Если вам нужно читать или записывать эти состояния, обратитесь к исходному коду ClickHouse для нужной функции.
:::

<div id="qbit">
  ### QBit
</div>

`QBit` — это векторный тип для эффективного lookup с разными уровнями точности. Внутренне он хранится в транспонированном формате. On the wire QBit — это просто `Array` базового типа элементов (`Int8`, `Float32`, `Float64` или `BFloat16`). Оптимизация хранения с битовым транспонированием выполняется на стороне сервера, а не в протоколе RowBinary.

Синтаксис:

```text
QBit(element_type, dimension[, stride])
```

Где `element_type` — `Int8`, `Float32`, `Float64` или `BFloat16`, а `dimension` — фиксированная размерность вектора. Необязательный параметр `stride` управляет только тем, как битовые плоскости группируются в потоки хранения на стороне сервера; на формат передачи данных RowBinary он не влияет, поскольку тот всегда представляет собой полный массив из `dimension` элементов.

Формат передачи данных: идентичен `Array(element_type)`:

```text
// LEB128 length
// followed by `length` elements of `element_type`
```

Пример кодирования `QBit(Float32, 4)`, содержащий `[1.0, 2.0, 3.0, 4.0]`:

```sql
SELECT [1.0, 2.0, 3.0, 4.0]::QBit(Float32, 4)
```

```text
0x04,                   // LEB128 - array has 4 elements
0x00, 0x00, 0x80, 0x3F, // 1.0 as Float32
0x00, 0x00, 0x00, 0x40, // 2.0 as Float32
0x00, 0x00, 0x40, 0x40, // 3.0 as Float32
0x00, 0x00, 0x80, 0x40, // 4.0 as Float32
```

<div id="format-settings">
  ## Настройки формата
</div>

<RowBinaryFormatSettings />