---
sidebar_position: 42
sidebar_label: "Функции поиска и замены в строках"
---

# Функции поиска и замены в строках {#funktsii-poiska-i-zameny-v-strokakh}

## replaceOne(haystack, pattern, replacement) {#replaceonehaystack-pattern-replacement}

Замена первого вхождения, если такое есть, подстроки pattern в haystack на подстроку replacement.
Здесь и далее, pattern и replacement должны быть константами.

## replaceAll(haystack, pattern, replacement) {#replaceallhaystack-pattern-replacement}

Замена всех вхождений подстроки pattern в haystack на подстроку replacement.

## replaceRegexpOne(haystack, pattern, replacement) {#replaceregexponehaystack-pattern-replacement}

Замена по регулярному выражению pattern. Регулярное выражение re2.
Заменяется только первое вхождение, если есть.
В качестве replacement может быть указан шаблон для замен. Этот шаблон может включать в себя подстановки `\0-\9`.
Подстановка `\0` - вхождение регулярного выражения целиком. Подстановки `\1-\9` - соответствующие по номеру subpattern-ы.
Для указания символа `\` в шаблоне, он должен быть экранирован с помощью символа `\`.
Также помните о том, что строковый литерал требует ещё одно экранирование.

Пример 1. Переведём дату в американский формат:

``` sql
SELECT DISTINCT
    EventDate,
    replaceRegexpOne(toString(EventDate), '(\\d{4})-(\\d{2})-(\\d{2})', '\\2/\\3/\\1') AS res
FROM test.hits
LIMIT 7
FORMAT TabSeparated
```

``` text
2014-03-17      03/17/2014
2014-03-18      03/18/2014
2014-03-19      03/19/2014
2014-03-20      03/20/2014
2014-03-21      03/21/2014
2014-03-22      03/22/2014
2014-03-23      03/23/2014
```

Пример 2. Размножить строку десять раз:

``` sql
SELECT replaceRegexpOne('Hello, World!', '.*', '\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0') AS res
```

``` text
┌─res────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World! │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## replaceRegexpAll(haystack, pattern, replacement) {#replaceregexpallhaystack-pattern-replacement}

То же самое, но делается замена всех вхождений. Пример:

``` sql
SELECT replaceRegexpAll('Hello, World!', '.', '\\0\\0') AS res
```

``` text
┌─res────────────────────────┐
│ HHeelllloo,,  WWoorrlldd!! │
└────────────────────────────┘
```

В качестве исключения, если регулярное выражение сработало на пустой подстроке, то замена делается не более одного раза.
Пример:

``` sql
SELECT replaceRegexpAll('Hello, World!', '^', 'here: ') AS res
```

``` text
┌─res─────────────────┐
│ here: Hello, World! │
└─────────────────────┘
```

## translate(s, from, to)

Данная функция заменяет символы в строке ‘s’ в соответствии с поэлементным отображением определяемым строками ‘from’ и ‘to’. ‘from’ и ‘to’ должны быть корректными ASCII строками одного размера. Не ASCII символы в оригинальной строке не изменяются.

Example:

``` sql
SELECT translate('Hello, World!', 'delor', 'DELOR') AS res
```

``` text
┌─res───────────┐
│ HELLO, WORLD! │
└───────────────┘
```

## translateUTF8(string, from, to)

Аналогично предыдущей функции, но работает со строками, состоящими из UTF-8 символов. ‘from’ и ‘to’ должны быть корректными UTF-8 строками одного размера.

Example:

``` sql
SELECT translateUTF8('Hélló, Wórld¡', 'óé¡', 'oe!') AS res
```

``` text
┌─res───────────┐
│ Hello, World! │
└───────────────┘
```
