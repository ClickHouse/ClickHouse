---
toc_priority: 68
toc_title: 时间窗口
---

# 时间窗口函数 {#time-window-han-shu}

时间窗口函数用于获取窗口的起始(包含边界)和结束时间(不包含边界)。系统支持的时间窗口函数如下：

## tumble {#time-window-functions-tumble}

tumble窗口是连续的、不重叠的固定大小(`interval`)时间窗口。

``` sql
tumble(time_attr, interval [, timezone])
```

**参数**
- `time_attr` - [DateTime](../../sql-reference/data-types/datetime.md)类型的时间数据。
- `interval` - [Interval](../../sql-reference/data-types/special-data-types/interval.md)类型的窗口大小。
- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) 类型的时区(可选参数). 

**返回值**

-  tumble窗口的开始(包含边界)和结束时间(不包含边界)

类型: `Tuple(DateTime, DateTime)`

**示例**

查询：

``` sql
SELECT tumble(now(), toIntervalDay('1'))
```

结果：

``` text
┌─tumble(now(), toIntervalDay('1'))─────────────┐
│ ['2020-01-01 00:00:00','2020-01-02 00:00:00'] │
└───────────────────────────────────────────────┘
```

## hop {#time-window-functions-hop}

hop窗口是一个固定大小(`window_interval`)的时间窗口，并按照一个固定的滑动间隔(`hop_interval`)滑动。当滑动间隔小于窗口大小时，滑动窗口间存在重叠，此时一个数据可能存在于多个窗口。

``` sql
hop(time_attr, hop_interval, window_interval [, timezone])
```

**参数**

- `time_attr` - [DateTime](../../sql-reference/data-types/datetime.md)类型的时间数据。
- `hop_interval` - [Interval](../../sql-reference/data-types/special-data-types/interval.md)类型的滑动间隔，需要大于0。
- `window_interval` - [Interval](../../sql-reference/data-types/special-data-types/interval.md)类型的窗口大小，需要大于0。
- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) 类型的时区(可选参数)。

**返回值**

- hop窗口的开始(包含边界)和结束时间(不包含边界)。由于一个数据可能存在于多个窗口，脱离window view单独调用该函数时只返回第一个窗口数据。

类型: `Tuple(DateTime, DateTime)`

**示例**

查询：

``` sql
SELECT hop(now(), INTERVAL '1' SECOND, INTERVAL '2' SECOND)
```

结果：

``` text
┌─hop(now(), toIntervalSecond('1'), toIntervalSecond('2'))──┐
│ ('2020-01-14 16:58:22','2020-01-14 16:58:24')             │
└───────────────────────────────────────────────────────────┘
```

## tumbleStart {#time-window-functions-tumblestart}

返回tumble窗口的开始时间(包含边界)。

``` sql
tumbleStart(time_attr, interval [, timezone]);
```

## tumbleEnd {#time-window-functions-tumbleend}

返回tumble窗口的结束时间(不包含边界)。

``` sql
tumbleEnd(time_attr, interval [, timezone]);
```

## hopStart {#time-window-functions-hopstart}

返回hop窗口的开始时间(包含边界)。

``` sql
hopStart(time_attr, hop_interval, window_interval [, timezone]);
```

## hopEnd {#time-window-functions-hopend}

返回hop窗口的结束时间(不包含边界)。

``` sql
hopEnd(time_attr, hop_interval, window_interval [, timezone]);
```