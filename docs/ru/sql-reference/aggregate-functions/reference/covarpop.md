---
toc_priority: 36
---

# covarPop {#covarpop}

Синтаксис: `covarPop(x, y)`

Вычисляет величину `Σ((x - x̅)(y - y̅)) / n`.

!!! note "Примечание"
    Функция использует вычислительно неустойчивый алгоритм. Если для ваших расчётов необходима [вычислительная устойчивость](https://ru.wikipedia.org/wiki/Вычислительная_устойчивость), используйте функцию `covarPopStable`. Она работает медленнее, но обеспечивает меньшую вычислительную ошибку.

[Оригинальная статья](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/covarpop/) <!--hide-->
