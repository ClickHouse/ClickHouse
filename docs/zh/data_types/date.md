# Date

Date 用两个字节来存储从 1970-01-01 到现在的日期值（无符号的值）。Date 允许存储的值 UNIX 纪元开始后的时间值，这个值上限会在编译阶段作为一个常量存储（当前只能到 2038 年，但可以拓展到 2106 年）。
Date 最小的值为 0000-00-00 00:00:00。

Date 中没有存储时区信息。

[来源文章](https://clickhouse.yandex/docs/en/data_types/date/) <!--hide-->
