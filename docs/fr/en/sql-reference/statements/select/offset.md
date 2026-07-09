---
description: 'Documentation de référence pour OFFSET'
sidebar_label: 'OFFSET'
slug: /sql-reference/statements/select/offset
title: 'Clause OFFSET FETCH'
doc_type: 'reference'
---

`OFFSET` et `FETCH` permettent de récupérer les données par portions. Ils spécifient un bloc de lignes à renvoyer en une seule requête.

```sql
-- SQL Standard style:
[OFFSET offset_row_count {ROW | ROWS}] [FETCH {FIRST | NEXT} fetch_row_count {ROW | ROWS} {ONLY | WITH TIES}]

-- MySQL/PostgreSQL style:
[LIMIT [n, ]m] [OFFSET offset_row_count]
```

La valeur `offset_row_count` ou `fetch_row_count` peut être un nombre ou une constante littérale. Vous pouvez omettre `fetch_row_count` ; par défaut, elle est égale à 1.

`OFFSET` indique le nombre de lignes à ignorer avant de commencer à renvoyer des lignes dans le jeu de résultats de la requête. `OFFSET n` ignore les `n` premières lignes du résultat.

Un `OFFSET` négatif est également pris en charge : `OFFSET -n` ignore les `n` dernières lignes du résultat.

Un `OFFSET` fractionnaire est lui aussi pris en charge : `OFFSET n` — si 0 &lt; n &lt; 1, alors les n * 100 % premiers du résultat sont ignorés.

Exemple :
• `OFFSET 0.1` — ignore les 10 % premiers du résultat.

> **Remarque**
> • La fraction doit être un nombre [Float64](../../data-types/float.md) inférieur à 1 et supérieur à zéro.
> • Si le calcul produit un nombre fractionnaire de lignes, il est arrondi à l’entier supérieur.

`FETCH` indique le nombre maximal de lignes pouvant figurer dans le résultat d’une requête.

L’option `ONLY` sert à renvoyer les lignes qui suivent immédiatement celles omises par `OFFSET`. Dans ce cas, `FETCH` constitue une alternative à la clause [LIMIT](../../../sql-reference/statements/select/limit.md). Par exemple, la requête suivante

```sql
SELECT * FROM test_fetch ORDER BY a OFFSET 1 ROW FETCH FIRST 3 ROWS ONLY;
```

est identique à la requête

```sql
SELECT * FROM test_fetch ORDER BY a LIMIT 3 OFFSET 1;
```

L’option `WITH TIES` permet de renvoyer les lignes supplémentaires ex æquo à la dernière position dans le jeu de résultats, conformément à la clause `ORDER BY`. Par exemple, si `fetch_row_count` est défini sur 5, mais que deux lignes supplémentaires correspondent aux valeurs des colonnes `ORDER BY` de la cinquième ligne, le jeu de résultats contiendra sept lignes.

:::note
Conformément à la norme, la clause `OFFSET` doit précéder la clause `FETCH` si les deux sont présentes.
:::

:::note
L’offset effectif peut également dépendre du paramètre [offset](../../../operations/settings/settings.md#offset).
:::

<div id="examples">
  ## Exemples
</div>

Table source :

```text
┌─a─┬─b─┐
│ 1 │ 1 │
│ 2 │ 1 │
│ 3 │ 4 │
│ 1 │ 3 │
│ 5 │ 4 │
│ 0 │ 6 │
│ 5 │ 7 │
└───┴───┘
```

Utilisation de l’option `ONLY` :

```sql title="Query"
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS ONLY;
```

```text title="Response"
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
└───┴───┘
```

Utilisation de l’option `WITH TIES` :

```sql title="Query"
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS WITH TIES;
```

```text title="Response"
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
│ 5 │ 7 │
└───┴───┘
```