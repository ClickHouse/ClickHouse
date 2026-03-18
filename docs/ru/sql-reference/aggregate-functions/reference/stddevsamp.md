---
slug: /ru/sql-reference/aggregate-functions/reference/stddevsamp
sidebar_position: 31
---

# stddevSamp {#stddevsamp}

Результат равен квадратному корню от `varSamp(x)`.

:::note Примечание
Функция использует вычислительно неустойчивый алгоритм. Если для ваших расчётов необходима [вычислительная устойчивость](https://ru.wikipedia.org/wiki/Вычислительная_устойчивость), используйте функцию `stddevSampStable`. Она работает медленнее, но обеспечивает меньшую вычислительную ошибку.
:::
