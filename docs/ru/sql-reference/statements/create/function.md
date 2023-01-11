---
sidebar_position: 38
sidebar_label: FUNCTION
---

# CREATE FUNCTION {#create-function}

Создает пользовательскую функцию из лямбда-выражения. Выражение должно состоять из параметров функции, констант, операторов и вызовов других функций.

**Синтаксис**

```sql
CREATE FUNCTION name AS (parameter0, ...) -> expression
```
У функции может быть произвольное число параметров.

Существует несколько ограничений на создаваемые функции:

-   Имя функции должно быть уникальным среди всех пользовательских и системных функций.
-   Рекурсивные функции запрещены.
-   Все переменные, используемые функцией, должны быть перечислены в списке ее параметров.

Если какое-нибудь ограничение нарушается, то при попытке создать функцию возникает исключение.

**Пример**

Запрос:

```sql
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
SELECT number, linear_equation(number, 2, 1) FROM numbers(3);
```

Результат:

``` text
┌─number─┬─plus(multiply(2, number), 1)─┐
│      0 │                            1 │
│      1 │                            3 │
│      2 │                            5 │
└────────┴──────────────────────────────┘
```

В следующем запросе пользовательская функция вызывает [условную функцию](../../../sql-reference/functions/conditional-functions.md):

```sql
CREATE FUNCTION parity_str AS (n) -> if(n % 2, 'odd', 'even');
SELECT number, parity_str(number) FROM numbers(3);
```

Результат:

``` text
┌─number─┬─if(modulo(number, 2), 'odd', 'even')─┐
│      0 │ even                                 │
│      1 │ odd                                  │
│      2 │ even                                 │
└────────┴──────────────────────────────────────┘
```
