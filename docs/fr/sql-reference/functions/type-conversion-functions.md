---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 38
toc_title: La Conversion De Type
---

# Fonctions De Conversion De Type {#type-conversion-functions}

## Problèmes courants des Conversions numériques {#numeric-conversion-issues}

Lorsque vous convertissez une valeur d'un type de données à un autre, vous devez vous rappeler que dans le cas courant, il s'agit d'une opération dangereuse qui peut entraîner une perte de données. Une perte de données peut se produire si vous essayez d'ajuster la valeur d'un type de données plus grand à un type de données plus petit, ou si vous convertissez des valeurs entre différents types de données.

ClickHouse a le [même comportement que les programmes C++ ](https://en.cppreference.com/w/cpp/language/implicit_conversion).

## toInt (8/16/32/64) {#toint8163264}

Convertit une valeur d'entrée en [Int](../../sql-reference/data-types/int-uint.md) type de données. Cette fonction comprend:

-   `toInt8(expr)` — Results in the `Int8` type de données.
-   `toInt16(expr)` — Results in the `Int16` type de données.
-   `toInt32(expr)` — Results in the `Int32` type de données.
-   `toInt64(expr)` — Results in the `Int64` type de données.

**Paramètre**

-   `expr` — [Expression](../syntax.md#syntax-expressions) renvoyer un nombre ou une chaîne avec la représentation décimale d'un nombre. Les représentations binaires, octales et hexadécimales des nombres ne sont pas prises en charge. Les zéros principaux sont dépouillés.

**Valeur renvoyée**

Valeur entière dans le `Int8`, `Int16`, `Int32`, ou `Int64` type de données.

Fonctions d'utilisation [l'arrondi vers zéro](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), ce qui signifie qu'ils tronquent des chiffres fractionnaires de nombres.

Le comportement des fonctions pour le [NaN et Inf](../../sql-reference/data-types/float.md#data_type-float-nan-inf) arguments est indéfini. Rappelez-vous sur [problèmes de conversion numérique](#numeric-conversion-issues), lorsque vous utilisez les fonctions.

**Exemple**

``` sql
SELECT toInt64(nan), toInt32(32), toInt16('16'), toInt8(8.8)
```

``` text
┌─────────toInt64(nan)─┬─toInt32(32)─┬─toInt16('16')─┬─toInt8(8.8)─┐
│ -9223372036854775808 │          32 │            16 │           8 │
└──────────────────────┴─────────────┴───────────────┴─────────────┘
```

## toInt (8/16/32/64)OrZero {#toint8163264orzero}

Il prend un argument de type String et essaie de l'analyser en Int (8 \| 16 \| 32 \| 64). En cas d'échec, renvoie 0.

**Exemple**

``` sql
select toInt64OrZero('123123'), toInt8OrZero('123qwe123')
```

``` text
┌─toInt64OrZero('123123')─┬─toInt8OrZero('123qwe123')─┐
│                  123123 │                         0 │
└─────────────────────────┴───────────────────────────┘
```

## toInt (8/16/32/64)OrNull {#toint8163264ornull}

Il prend un argument de type String et essaie de l'analyser en Int (8 \| 16 \| 32 \| 64). En cas d'échec, renvoie NULL.

**Exemple**

``` sql
select toInt64OrNull('123123'), toInt8OrNull('123qwe123')
```

``` text
┌─toInt64OrNull('123123')─┬─toInt8OrNull('123qwe123')─┐
│                  123123 │                      ᴺᵁᴸᴸ │
└─────────────────────────┴───────────────────────────┘
```

## toUInt (8/16/32/64) {#touint8163264}

Convertit une valeur d'entrée en [UInt](../../sql-reference/data-types/int-uint.md) type de données. Cette fonction comprend:

-   `toUInt8(expr)` — Results in the `UInt8` type de données.
-   `toUInt16(expr)` — Results in the `UInt16` type de données.
-   `toUInt32(expr)` — Results in the `UInt32` type de données.
-   `toUInt64(expr)` — Results in the `UInt64` type de données.

**Paramètre**

-   `expr` — [Expression](../syntax.md#syntax-expressions) renvoyer un nombre ou une chaîne avec la représentation décimale d'un nombre. Les représentations binaires, octales et hexadécimales des nombres ne sont pas prises en charge. Les zéros principaux sont dépouillés.

**Valeur renvoyée**

Valeur entière dans le `UInt8`, `UInt16`, `UInt32`, ou `UInt64` type de données.

Fonctions d'utilisation [l'arrondi vers zéro](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), ce qui signifie qu'ils tronquent des chiffres fractionnaires de nombres.

Le comportement des fonctions pour les agruments négatifs et pour le [NaN et Inf](../../sql-reference/data-types/float.md#data_type-float-nan-inf) arguments est indéfini. Si vous passez une chaîne avec un nombre négatif, par exemple `'-32'`, ClickHouse soulève une exception. Rappelez-vous sur [problèmes de conversion numérique](#numeric-conversion-issues), lorsque vous utilisez les fonctions.

**Exemple**

``` sql
SELECT toUInt64(nan), toUInt32(-32), toUInt16('16'), toUInt8(8.8)
```

``` text
┌───────toUInt64(nan)─┬─toUInt32(-32)─┬─toUInt16('16')─┬─toUInt8(8.8)─┐
│ 9223372036854775808 │    4294967264 │             16 │            8 │
└─────────────────────┴───────────────┴────────────────┴──────────────┘
```

## toUInt (8/16/32/64)OrZero {#touint8163264orzero}

## toUInt (8/16/32/64)OrNull {#touint8163264ornull}

## toFloat (32/64) {#tofloat3264}

## toFloat (32/64)OrZero {#tofloat3264orzero}

## toFloat (32/64) OrNull {#tofloat3264ornull}

## jour {#todate}

## toDateOrZero {#todateorzero}

## toDateOrNull {#todateornull}

## toDateTime {#todatetime}

## toDateTimeOrZero {#todatetimeorzero}

## toDateTimeOrNull {#todatetimeornull}

## toDecimal (32/64/128) {#todecimal3264128}

Convertir `value` à l' [Décimal](../../sql-reference/data-types/decimal.md) type de données avec précision de `S`. Le `value` peut être un nombre ou une chaîne. Le `S` (l'échelle) paramètre spécifie le nombre de décimales.

-   `toDecimal32(value, S)`
-   `toDecimal64(value, S)`
-   `toDecimal128(value, S)`

## toDecimal (32/64/128) OrNull {#todecimal3264128ornull}

Convertit une chaîne d'entrée en [Nullable (Décimal (P, S))](../../sql-reference/data-types/decimal.md) valeur de type de données. Cette famille de fonctions comprennent:

-   `toDecimal32OrNull(expr, S)` — Results in `Nullable(Decimal32(S))` type de données.
-   `toDecimal64OrNull(expr, S)` — Results in `Nullable(Decimal64(S))` type de données.
-   `toDecimal128OrNull(expr, S)` — Results in `Nullable(Decimal128(S))` type de données.

Ces fonctions devraient être utilisées à la place de `toDecimal*()` fonctions, si vous préférez obtenir un `NULL` la valeur au lieu d'une exception dans le cas d'une valeur d'entrée erreur d'analyse.

**Paramètre**

-   `expr` — [Expression](../syntax.md#syntax-expressions), retourne une valeur dans l' [Chaîne](../../sql-reference/data-types/string.md) type de données. ClickHouse attend la représentation textuelle du nombre décimal. Exemple, `'1.111'`.
-   `S` — Scale, the number of decimal places in the resulting value.

**Valeur renvoyée**

Une valeur dans l' `Nullable(Decimal(P,S))` type de données. La valeur contient:

-   Numéro `S` décimales, si ClickHouse interprète la chaîne d'entrée comme un nombre.
-   `NULL` si ClickHouse ne peut pas interpréter la chaîne d'entrée comme un nombre ou si le nombre d'entrée contient plus de `S` décimale.

**Exemple**

``` sql
SELECT toDecimal32OrNull(toString(-1.111), 5) AS val, toTypeName(val)
```

``` text
┌──────val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 5))─┐
│ -1.11100 │ Nullable(Decimal(9, 5))                            │
└──────────┴────────────────────────────────────────────────────┘
```

``` sql
SELECT toDecimal32OrNull(toString(-1.111), 2) AS val, toTypeName(val)
```

``` text
┌──val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 2))─┐
│ ᴺᵁᴸᴸ │ Nullable(Decimal(9, 2))                            │
└──────┴────────────────────────────────────────────────────┘
```

## toDecimal (32/64/128)OrZero {#todecimal3264128orzero}

Convertit une valeur d'entrée en [Decimal(P,S)](../../sql-reference/data-types/decimal.md) type de données. Cette famille de fonctions comprennent:

-   `toDecimal32OrZero( expr, S)` — Results in `Decimal32(S)` type de données.
-   `toDecimal64OrZero( expr, S)` — Results in `Decimal64(S)` type de données.
-   `toDecimal128OrZero( expr, S)` — Results in `Decimal128(S)` type de données.

Ces fonctions devraient être utilisées à la place de `toDecimal*()` fonctions, si vous préférez obtenir un `0` la valeur au lieu d'une exception dans le cas d'une valeur d'entrée erreur d'analyse.

**Paramètre**

-   `expr` — [Expression](../syntax.md#syntax-expressions), retourne une valeur dans l' [Chaîne](../../sql-reference/data-types/string.md) type de données. ClickHouse attend la représentation textuelle du nombre décimal. Exemple, `'1.111'`.
-   `S` — Scale, the number of decimal places in the resulting value.

**Valeur renvoyée**

Une valeur dans l' `Nullable(Decimal(P,S))` type de données. La valeur contient:

-   Numéro `S` décimales, si ClickHouse interprète la chaîne d'entrée comme un nombre.
-   0 avec `S` décimales, si ClickHouse ne peut pas interpréter la chaîne d'entrée comme un nombre ou si le nombre d'entrée contient plus de `S` décimale.

**Exemple**

``` sql
SELECT toDecimal32OrZero(toString(-1.111), 5) AS val, toTypeName(val)
```

``` text
┌──────val─┬─toTypeName(toDecimal32OrZero(toString(-1.111), 5))─┐
│ -1.11100 │ Decimal(9, 5)                                      │
└──────────┴────────────────────────────────────────────────────┘
```

``` sql
SELECT toDecimal32OrZero(toString(-1.111), 2) AS val, toTypeName(val)
```

``` text
┌──val─┬─toTypeName(toDecimal32OrZero(toString(-1.111), 2))─┐
│ 0.00 │ Decimal(9, 2)                                      │
└──────┴────────────────────────────────────────────────────┘
```

## toString {#tostring}

Fonctions de conversion entre des nombres, des chaînes (mais pas des chaînes fixes), des dates et des dates avec des heures.
Toutes ces fonctions acceptent un argument.

Lors de la conversion vers ou à partir d'une chaîne, la valeur est formatée ou analysée en utilisant les mêmes règles que pour le format TabSeparated (et presque tous les autres formats de texte). Si la chaîne ne peut pas être analysée, une exception est levée et la demande est annulée.

Lors de la conversion de dates en nombres ou vice versa, la date correspond au nombre de jours depuis le début de L'époque Unix.
Lors de la conversion de dates avec des heures en nombres ou vice versa, la date avec l'heure correspond au nombre de secondes depuis le début de L'époque Unix.

Les formats date et date-avec-heure pour les fonctions toDate/toDateTime sont définis comme suit:

``` text
YYYY-MM-DD
YYYY-MM-DD hh:mm:ss
```

À titre d'exception, si vous convertissez des types numériques UInt32, Int32, UInt64 ou Int64 à Date, et si le nombre est supérieur ou égal à 65536, le nombre est interprété comme un horodatage Unix (et non comme le nombre de jours) et est arrondi à la date. Cela permet de prendre en charge l'occurrence commune de l'écriture ‘toDate(unix_timestamp)’, qui autrement serait une erreur et nécessiterait d'écrire le plus lourd ‘toDate(toDateTime(unix_timestamp))’.

La Conversion entre une date et une date avec l'heure est effectuée de manière naturelle: en ajoutant une heure nulle ou en supprimant l'heure.

La Conversion entre types numériques utilise les mêmes règles que les affectations entre différents types numériques en C++.

De plus, la fonction ToString de L'argument DateTime peut prendre un deuxième argument de chaîne contenant le nom du fuseau horaire. Exemple: `Asia/Yekaterinburg` Dans ce cas, l'heure est formatée en fonction du fuseau horaire spécifié.

``` sql
SELECT
    now() AS now_local,
    toString(now(), 'Asia/Yekaterinburg') AS now_yekat
```

``` text
┌───────────now_local─┬─now_yekat───────────┐
│ 2016-06-15 00:11:21 │ 2016-06-15 02:11:21 │
└─────────────────────┴─────────────────────┘
```

Voir aussi l' `toUnixTimestamp` fonction.

## toFixedString (s, N) {#tofixedstrings-n}

Convertit un argument de type String en un type FixedString (N) (une chaîne de longueur fixe N). N doit être une constante.
Si la chaîne a moins d'octets que N, elle est complétée avec des octets null à droite. Si la chaîne a plus d'octets que N, une exception est levée.

## toStringCutToZero(s) {#tostringcuttozeros}

Accepte un argument String ou FixedString. Renvoie la chaîne avec le contenu tronqué au premier octet zéro trouvé.

Exemple:

``` sql
SELECT toFixedString('foo', 8) AS s, toStringCutToZero(s) AS s_cut
```

``` text
┌─s─────────────┬─s_cut─┐
│ foo\0\0\0\0\0 │ foo   │
└───────────────┴───────┘
```

``` sql
SELECT toFixedString('foo\0bar', 8) AS s, toStringCutToZero(s) AS s_cut
```

``` text
┌─s──────────┬─s_cut─┐
│ foo\0bar\0 │ foo   │
└────────────┴───────┘
```

## reinterpretAsUInt (8/16/32/64) {#reinterpretasuint8163264}

## reinterpretAsInt (8/16/32/64) {#reinterpretasint8163264}

## reinterpretAsFloat (32/64) {#reinterpretasfloat3264}

## réinterprétasdate {#reinterpretasdate}

## reinterpretAsDateTime {#reinterpretasdatetime}

Ces fonctions acceptent une chaîne et interprètent les octets placés au début de la chaîne comme un nombre dans l'ordre de l'hôte (little endian). Si la chaîne n'est pas assez longue, les fonctions fonctionnent comme si la chaîne était remplie avec le nombre nécessaire d'octets nuls. Si la chaîne est plus longue que nécessaire, les octets supplémentaires sont ignorés. Une date est interprétée comme le nombre de jours depuis le début de l'Époque Unix, et une date avec le temps, est interprété comme le nombre de secondes écoulées depuis le début de l'Époque Unix.

## reinterpretAsString {#type_conversion_functions-reinterpretAsString}

Cette fonction accepte un nombre ou une date ou une date avec l'heure, et renvoie une chaîne contenant des octets représentant la valeur correspondante dans l'ordre de l'hôte (little endian). Les octets nuls sont supprimés de la fin. Par exemple, une valeur de type uint32 de 255 est une chaîne longue d'un octet.

## reinterpretAsFixedString {#reinterpretasfixedstring}

Cette fonction accepte un nombre ou une date ou une date avec l'heure, et renvoie une chaîne fixe contenant des octets représentant la valeur correspondante dans l'ordre de l'hôte (little endian). Les octets nuls sont supprimés de la fin. Par exemple, une valeur de type uint32 de 255 est une chaîne fixe longue d'un octet.

## CAST (x, T) {#type_conversion_function-cast}

Convertir ‘x’ à l' ‘t’ type de données. La syntaxe CAST (X comme t) est également prise en charge.

Exemple:

``` sql
SELECT
    '2016-06-15 23:00:00' AS timestamp,
    CAST(timestamp AS DateTime) AS datetime,
    CAST(timestamp AS Date) AS date,
    CAST(timestamp, 'String') AS string,
    CAST(timestamp, 'FixedString(22)') AS fixed_string
```

``` text
┌─timestamp───────────┬────────────datetime─┬───────date─┬─string──────────────┬─fixed_string──────────────┐
│ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00\0\0\0 │
└─────────────────────┴─────────────────────┴────────────┴─────────────────────┴───────────────────────────┘
```

La Conversion en FixedString (N) ne fonctionne que pour les arguments de type String ou FixedString (N).

Type conversion en [Nullable](../../sql-reference/data-types/nullable.md) et le dos est pris en charge. Exemple:

``` sql
SELECT toTypeName(x) FROM t_null
```

``` text
┌─toTypeName(x)─┐
│ Int8          │
│ Int8          │
└───────────────┘
```

``` sql
SELECT toTypeName(CAST(x, 'Nullable(UInt16)')) FROM t_null
```

``` text
┌─toTypeName(CAST(x, 'Nullable(UInt16)'))─┐
│ Nullable(UInt16)                        │
│ Nullable(UInt16)                        │
└─────────────────────────────────────────┘
```

## toInterval (année / trimestre / Mois / Semaine / Jour / Heure / Minute / Seconde) {#function-tointerval}

Convertit un argument de type Number en [Intervalle](../../sql-reference/data-types/special-data-types/interval.md) type de données.

**Syntaxe**

``` sql
toIntervalSecond(number)
toIntervalMinute(number)
toIntervalHour(number)
toIntervalDay(number)
toIntervalWeek(number)
toIntervalMonth(number)
toIntervalQuarter(number)
toIntervalYear(number)
```

**Paramètre**

-   `number` — Duration of interval. Positive integer number.

**Valeurs renvoyées**

-   La valeur de `Interval` type de données.

**Exemple**

``` sql
WITH
    toDate('2019-01-01') AS date,
    INTERVAL 1 WEEK AS interval_week,
    toIntervalWeek(1) AS interval_to_week
SELECT
    date + interval_week,
    date + interval_to_week
```

``` text
┌─plus(date, interval_week)─┬─plus(date, interval_to_week)─┐
│                2019-01-08 │                   2019-01-08 │
└───────────────────────────┴──────────────────────────────┘
```

## parseDateTimeBestEffort {#parsedatetimebesteffort}

Convertit une date et une heure dans le [Chaîne](../../sql-reference/data-types/string.md) la représentation de [DateTime](../../sql-reference/data-types/datetime.md#data_type-datetime) type de données.

La fonction d'analyse [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601), [RFC 1123 - 5.2.14 RFC-822 date et heure Spécification](https://tools.ietf.org/html/rfc1123#page-55), ClickHouse et d'autres formats de date et d'heure.

**Syntaxe**

``` sql
parseDateTimeBestEffort(time_string [, time_zone]);
```

**Paramètre**

-   `time_string` — String containing a date and time to convert. [Chaîne](../../sql-reference/data-types/string.md).
-   `time_zone` — Time zone. The function parses `time_string` selon le fuseau horaire. [Chaîne](../../sql-reference/data-types/string.md).

**Formats non standard pris en charge**

-   Une chaîne contenant 9..10 chiffres [le timestamp unix](https://en.wikipedia.org/wiki/Unix_time).
-   Une chaîne avec une date et une heure composant: `YYYYMMDDhhmmss`, `DD/MM/YYYY hh:mm:ss`, `DD-MM-YY hh:mm`, `YYYY-MM-DD hh:mm:ss`, etc.
-   Une chaîne avec une date, mais pas de composant de temps: `YYYY`, `YYYYMM`, `YYYY*MM`, `DD/MM/YYYY`, `DD-MM-YY` etc.
-   Une chaîne avec un jour et une heure: `DD`, `DD hh`, `DD hh:mm`. Dans ce cas `YYYY-MM` sont substitués comme suit `2000-01`.
-   Une chaîne qui inclut la date et l'heure ainsi que des informations de décalage de fuseau horaire: `YYYY-MM-DD hh:mm:ss ±h:mm`, etc. Exemple, `2020-12-12 17:36:00 -5:00`.

Pour tous les formats avec séparateur, la fonction analyse les noms de mois exprimés par leur nom complet ou par les trois premières lettres d'un nom de mois. Exemple: `24/DEC/18`, `24-Dec-18`, `01-September-2018`.

**Valeur renvoyée**

-   `time_string` converti à l' `DateTime` type de données.

**Exemple**

Requête:

``` sql
SELECT parseDateTimeBestEffort('12/12/2020 12:12:57')
AS parseDateTimeBestEffort;
```

Résultat:

``` text
┌─parseDateTimeBestEffort─┐
│     2020-12-12 12:12:57 │
└─────────────────────────┘
```

Requête:

``` sql
SELECT parseDateTimeBestEffort('Sat, 18 Aug 2018 07:22:16 GMT', 'Europe/Moscow')
AS parseDateTimeBestEffort
```

Résultat:

``` text
┌─parseDateTimeBestEffort─┐
│     2018-08-18 10:22:16 │
└─────────────────────────┘
```

Requête:

``` sql
SELECT parseDateTimeBestEffort('1284101485')
AS parseDateTimeBestEffort
```

Résultat:

``` text
┌─parseDateTimeBestEffort─┐
│     2015-07-07 12:04:41 │
└─────────────────────────┘
```

Requête:

``` sql
SELECT parseDateTimeBestEffort('2018-12-12 10:12:12')
AS parseDateTimeBestEffort
```

Résultat:

``` text
┌─parseDateTimeBestEffort─┐
│     2018-12-12 10:12:12 │
└─────────────────────────┘
```

Requête:

``` sql
SELECT parseDateTimeBestEffort('10 20:19')
```

Résultat:

``` text
┌─parseDateTimeBestEffort('10 20:19')─┐
│                 2000-01-10 20:19:00 │
└─────────────────────────────────────┘
```

**Voir Aussi**

-   \[Annonce ISO 8601 par @xkcd\](https://xkcd.com/1179/)
-   [RFC 1123](https://tools.ietf.org/html/rfc1123)
-   [jour](#todate)
-   [toDateTime](#todatetime)

## parseDateTimeBestEffortOrNull {#parsedatetimebesteffortornull}

De même que pour [parseDateTimeBestEffort](#parsedatetimebesteffort) sauf qu'il renvoie null lorsqu'il rencontre un format de date qui ne peut pas être traité.

## parseDateTimeBestEffortOrZero {#parsedatetimebesteffortorzero}

De même que pour [parseDateTimeBestEffort](#parsedatetimebesteffort) sauf qu'il renvoie une date zéro ou une date zéro lorsqu'il rencontre un format de date qui ne peut pas être traité.

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/type_conversion_functions/) <!--hide-->
