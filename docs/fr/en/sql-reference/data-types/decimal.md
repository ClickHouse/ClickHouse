---
description: 'Documentation sur les types de données Decimal dans ClickHouse, qui permettent
  une arithmétique à virgule fixe avec une précision configurable'
sidebar_label: 'Decimal'
sidebar_position: 6
slug: /sql-reference/data-types/decimal
title: 'Decimal, Decimal(P), Decimal(P, S), Decimal32(S), Decimal64(S), Decimal128(S),
  Decimal256(S)'
doc_type: 'reference'
---

Nombres signés à virgule fixe qui conservent leur précision lors des opérations d’addition, de soustraction et de multiplication. Pour la division, les chiffres de poids faible sont tronqués (et non arrondis).

<div id="parameters">
  ## Paramètres
</div>

* P - précision. Plage valide : [ 1 : 76 ]. Détermine le nombre de chiffres décimaux que le nombre peut avoir (y compris la partie fractionnaire). Par défaut, la précision est de 10.
* S - échelle. Plage valide : [ 0 : P ]. Détermine le nombre de chiffres décimaux que la partie fractionnaire peut avoir.

Decimal(P) est équivalent à Decimal(P, 0). De même, la syntaxe Decimal est équivalente à Decimal(10, 0).

Selon la valeur du paramètre P, Decimal(P, S) est un synonyme de :

* P compris entre [ 1 : 9 ] - pour Decimal32(S)
* P compris entre [ 10 : 18 ] - pour Decimal64(S)
* P compris entre [ 19 : 38 ] - pour Decimal128(S)
* P compris entre [ 39 : 76 ] - pour Decimal256(S)

<div id="decimal-value-ranges">
  ## Plages de valeurs décimales
</div>

* Decimal(P, S) - ( -1 * 10^(P - S), 1 * 10^(P - S) )
* Decimal32(S) - ( -1 * 10^(9 - S), 1 * 10^(9 - S) )
* Decimal64(S) - ( -1 * 10^(18 - S), 1 * 10^(18 - S) )
* Decimal128(S) - ( -1 * 10^(38 - S), 1 * 10^(38 - S) )
* Decimal256(S) - ( -1 * 10^(76 - S), 1 * 10^(76 - S) )

Par exemple, Decimal32(4) peut contenir des nombres compris entre -99999.9999 et 99999.9999, avec un pas de 0.0001.

<div id="internal-representation">
  ## Représentation interne
</div>

En interne, les données sont représentées sous forme d&#39;entiers signés ordinaires, avec la largeur de bits correspondante. Les plages de valeurs réelles pouvant être stockées en mémoire sont légèrement plus étendues que celles indiquées ci-dessus, et ne sont vérifiées que lors de la conversion depuis une chaîne.

Comme les CPU modernes ne prennent pas en charge nativement les entiers de 128 et 256 bits, les opérations sur Decimal128 et Decimal256 sont émulées. Par conséquent, Decimal128 et Decimal256 sont nettement plus lents que Decimal32/Decimal64.

<div id="operations-and-result-type">
  ## Opérations et type de résultat
</div>

Les opérations binaires sur `Decimal` produisent un type de résultat plus large (quel que soit l&#39;ordre des arguments).

* `Decimal64(S1) <op> Decimal32(S2) -> Decimal64(S)`
* `Decimal128(S1) <op> Decimal32(S2) -> Decimal128(S)`
* `Decimal128(S1) <op> Decimal64(S2) -> Decimal128(S)`
* `Decimal256(S1) <op> Decimal<32|64|128>(S2) -> Decimal256(S)`

Règles d’échelle :

* addition, soustraction : S = max(S1, S2).
* multiplication : S = S1 + S2.
* division : S = S1.

Pour des opérations similaires entre `Decimal` et des entiers, le résultat est un `Decimal` de la même taille que l&#39;argument.

Les opérations entre `Decimal` et `Float32`/`Float64` ne sont pas définies. Si vous en avez besoin, vous pouvez convertir explicitement l&#39;un des arguments à l&#39;aide des fonctions intégrées toDecimal32, toDecimal64, toDecimal128 ou toFloat32, toFloat64. Gardez à l&#39;esprit que le résultat perdra en précision et que la conversion de type est une opération coûteuse en calcul.

Certaines fonctions appliquées à `Decimal` renvoient un résultat en `Float64` (par exemple, var ou stddev). Les calculs intermédiaires peuvent néanmoins être effectués en `Decimal`, ce qui peut conduire à des résultats différents entre des entrées `Float64` et `Decimal` ayant les mêmes valeurs.

<div id="overflow-checks">
  ## Vérification des dépassements de capacité
</div>

Lors de calculs sur des valeurs `Decimal`, des dépassements de capacité d’entier peuvent se produire. Les chiffres excédentaires dans la partie fractionnaire sont supprimés (et non arrondis). Les chiffres excédentaires dans la partie entière entraînent une exception.

:::warning
La vérification des dépassements de capacité n’est pas implémentée pour `Decimal128` et `Decimal256`. En cas de dépassement de capacité, un résultat incorrect est renvoyé ; aucune exception n’est levée.
:::

```sql
SELECT toDecimal32(2, 4) AS x, x / 3
```

```text
┌──────x─┬─divide(toDecimal32(2, 4), 3)─┐
│ 2.0000 │                       0.6666 │
└────────┴──────────────────────────────┘
```

```sql
SELECT toDecimal32(4.2, 8) AS x, x * x
```

```text
DB::Exception: Scale is out of bounds.
```

```sql
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```

```text
DB::Exception: Decimal math overflow.
```

Les vérifications de dépassement ralentissent les opérations. S&#39;il est certain qu&#39;aucun dépassement n&#39;est possible, il est judicieux de désactiver ces vérifications à l&#39;aide du paramètre `decimal_check_overflow`. Lorsque les vérifications sont désactivées et qu&#39;un dépassement se produit, le résultat sera incorrect :

```sql
SET decimal_check_overflow = 0;
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```

```text
┌──────────x─┬─multiply(6, toDecimal32(4.2, 8))─┐
│ 4.20000000 │                     -17.74967296 │
└────────────┴──────────────────────────────────┘
```

Les vérifications de dépassement ont lieu non seulement lors des opérations arithmétiques, mais aussi lors de la comparaison de valeurs :

```sql
SELECT toDecimal32(1, 8) < 100
```

```text
DB::Exception: Can't compare.
```

**Voir aussi**

* [isDecimalOverflow](/fr/sql-reference/functions/other-functions#isDecimalOverflow)
* [countDigits](/fr/sql-reference/functions/other-functions#countDigits)