# 随机函数 {#sui-ji-han-shu}

随机函数使用非加密方式生成伪随机数字。

所有随机函数都只接受一个参数或不接受任何参数。
您可以向它传递任何类型的参数，但传递的参数将不会使用在任何随机数生成过程中。
此参数的唯一目的是防止公共子表达式消除，以便在相同的查询中使用相同的随机函数生成不同的随机数。

## rand, rand32 {#rand}

返回一个UInt32类型的随机数字，所有UInt32类型的数字被生成的概率均相等。此函数线性同于的方式生成随机数。

## rand64 {#rand64}

返回一个UInt64类型的随机数字，所有UInt64类型的数字被生成的概率均相等。此函数线性同于的方式生成随机数。

## randConstant {#randconstant}

返回一个UInt32类型的随机数字，该函数不同之处在于仅为每个数据块参数一个随机数。

[来源文章](https://clickhouse.com/docs/en/query_language/functions/random_functions/) <!--hide-->
