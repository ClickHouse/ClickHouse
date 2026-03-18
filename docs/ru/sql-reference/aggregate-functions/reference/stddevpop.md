---
slug: /ru/sql-reference/aggregate-functions/reference/stddevpop
sidebar_position: 30
---

# stddevPop {#stddevpop}

Результат равен квадратному корню от `varPop(x)`.

:::note Примечание
Функция использует вычислительно неустойчивый алгоритм. Если для ваших расчётов необходима [вычислительная устойчивость](https://ru.wikipedia.org/wiki/Вычислительная_устойчивость), используйте функцию `stddevPopStable`. Она работает медленнее, но обеспечивает меньшую вычислительную ошибку.
:::
