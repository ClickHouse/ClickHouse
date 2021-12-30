---
toc_priority: 68
toc_title: Time Window
---

# Time Window Functions {#time-window-functions}

Функции окна времени возвращают включенный нижний и исключенный верхний пределы соответствующего окна. Ниже указаны функции, которые работают с оконным представлением:

## tumble {#time-window-functions-tumble}

Переворачивающееся временное окно назначает записи неперекрывающимся непрерывным окнам с фиксированной продолжительностью (`interval`). 

``` sql
tumble(time_attr, interval [, timezone])
```

**Аргументы**

-  `time_attr` — дата и время. Тип данных [DateTime](../ru/sql-reference/data-types/datetime.md).
-  `interval` — интервал окна в типе данных [Interval](../ru/sql-reference/data-types/special-data-types/interval.md).
-  `timezone` — [имя временной зоны](../ru/operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) (необязательно). 

**Возвращаемые значения**

- Включенный нижний и исключенный верхний пределы соответствующего переворачивающегося окна.

Type: `Tuple(DateTime, DateTime)`

**Пример**

Запрос:

``` sql
SELECT tumble(now(), toIntervalDay('1'));
```

Результат:

``` text
┌─tumble(now(), toIntervalDay('1'))─────────────┐
│ ['2020-01-01 00:00:00','2020-01-02 00:00:00'] │
└───────────────────────────────────────────────┘
```

## hop {#time-window-functions-hop}

У прыгающего окна времени есть фиксированная продолжительность (`window_interval`), и оно прыгает на определенный прыжковый интервал (`hop_interval`). Если `hop_interval` меньше чем `window_interval`, прыгающие окна перекрывают друг друга. Таким образом, записи можно назначать множеству окон. 

``` sql
hop(time_attr, hop_interval, window_interval [, timezone])
```

**Аргументы**

- `time_attr` — дата и время. Тип данных [DateTime](../../sql-reference/data-types/datetime.md).
- `hop_interval` — интервал прыжка в типе данных [Interval](../../sql-reference/data-types/special-data-types/interval.md). Лучше взять положительное число.
- `window_interval` — интервал окна в типе данных [Interval](../../sql-reference/data-types/special-data-types/interval.md). Лучше взять положительное число.
- `timezone` — [имя временной зоны](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) (необязательно). 

**Возвращаемые значения**

- Включенный нижний и исключенный верхний пределы соответствующего прыгающего окна. Так как одну запись можно назначить множеству прыгающих окон, эта функция возвращает предел **первого** окна, если функцию прыжка использовать **без** `WINDOW VIEW`.

Тип: `Tuple(DateTime, DateTime)`

**Пример**

Запрос:

``` sql
SELECT hop(now(), INTERVAL '1' SECOND, INTERVAL '2' SECOND);
```

Результат:

``` text
┌─hop(now(), toIntervalSecond('1'), toIntervalSecond('2'))──┐
│ ('2020-01-14 16:58:22','2020-01-14 16:58:24')             │
└───────────────────────────────────────────────────────────┘
```

## tumbleStart {#time-window-functions-tumblestart}

Возвращает включенный нижний предел соответствующего переворачивающегося окна. 

``` sql
tumbleStart(time_attr, interval [, timezone]);
```

## tumbleEnd {#time-window-functions-tumbleend}

Возвращает исключенный верхний предел соответствующего переворачивающегося окна.

``` sql
tumbleEnd(time_attr, interval [, timezone]);
```

## hopStart {#time-window-functions-hopstart}

Возвращает включенный нижний предел соответствующего прыгающего окна.

``` sql
hopStart(time_attr, hop_interval, window_interval [, timezone]);
```

## hopEnd {#time-window-functions-hopend}

Возвращает исключенный верхний предел соответствующего прыгающего окна.

``` sql
hopEnd(time_attr, hop_interval, window_interval [, timezone]);
```