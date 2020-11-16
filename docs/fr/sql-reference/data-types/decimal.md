---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 42
toc_title: "D\xE9cimal"
---

# Décimal (P, S), Décimal32 (S), Décimal64 (S), Décimal128 (S) {#decimalp-s-decimal32s-decimal64s-decimal128s}

Nombres à points fixes signés qui conservent la précision pendant les opérations d'addition, de soustraction et de multiplication. Pour la division, les chiffres les moins significatifs sont ignorés (non arrondis).

## Paramètre {#parameters}

-   P-précision. Plage valide: \[1: 38 \]. Détermine le nombre de chiffres décimaux nombre peut avoir (fraction y compris).
-   S - échelle. Plage valide: \[0: P \]. Détermine le nombre de chiffres décimaux fraction peut avoir.

En fonction de P Paramètre Valeur décimal (P, S) est un synonyme de:
- P à partir de \[ 1: 9\] - Pour Décimal32 (S)
- P à partir de \[10: 18\] - pour Décimal64 (S)
- P à partir de \[19: 38\] - pour Décimal128 (S)

## Plages De Valeurs Décimales {#decimal-value-ranges}

-   Décimal32 (S) - ( -1 \* 10^(9 - S), 1 \* 10^(9-S) )
-   Décimal64 (S) - ( -1 \* 10^(18 - S), 1 \* 10^(18-S) )
-   Décimal128 (S) - ( -1 \* 10^(38 - S), 1 \* 10^(38-S) )

Par exemple, Decimal32(4) peut contenir des nombres de -99999.9999 à 99999.9999 avec 0,0001 étape.

## Représentation Interne {#internal-representation}

En interne, les données sont représentées comme des entiers signés normaux avec une largeur de bit respective. Les plages de valeurs réelles qui peuvent être stockées en mémoire sont un peu plus grandes que celles spécifiées ci-dessus, qui sont vérifiées uniquement lors de la conversion à partir d'une chaîne.

Parce que les processeurs modernes ne prennent pas en charge les entiers 128 bits nativement, les opérations sur Decimal128 sont émulées. Pour cette raison, Decimal128 fonctionne significativement plus lentement que Decimal32 / Decimal64.

## Opérations et type de résultat {#operations-and-result-type}

Les opérations binaires sur le résultat décimal dans le type de résultat plus large (avec n'importe quel ordre d'arguments).

-   `Decimal64(S1) <op> Decimal32(S2) -> Decimal64(S)`
-   `Decimal128(S1) <op> Decimal32(S2) -> Decimal128(S)`
-   `Decimal128(S1) <op> Decimal64(S2) -> Decimal128(S)`

Règles pour l'échelle:

-   ajouter, soustraire: S = max (S1, S2).
-   multuply: S = S1 + S2.
-   diviser: S = S1.

Pour des opérations similaires entre décimal et entier, le résultat est Décimal de la même taille qu'un argument.

Les opérations entre Decimal et Float32 / Float64 ne sont pas définies. Si vous en avez besoin, vous pouvez explicitement lancer l'un des arguments en utilisant les builtins toDecimal32, toDecimal64, toDecimal128 ou toFloat32, toFloat64. Gardez à l'esprit que le résultat perdra de la précision et que la conversion de type est une opération coûteuse en calcul.

Certaines fonctions sur le résultat de retour décimal comme Float64 (par exemple, var ou stddev). Les calculs intermédiaires peuvent toujours être effectués en décimal, ce qui peut conduire à des résultats différents entre les entrées Float64 et Decimal avec les mêmes valeurs.

## Contrôles De Débordement {#overflow-checks}

Pendant les calculs sur Décimal, des débordements entiers peuvent se produire. Les chiffres excessifs dans une fraction sont éliminés (non arrondis). Les chiffres excessifs dans la partie entière conduiront à une exception.

``` sql
SELECT toDecimal32(2, 4) AS x, x / 3
```

``` text
┌──────x─┬─divide(toDecimal32(2, 4), 3)─┐
│ 2.0000 │                       0.6666 │
└────────┴──────────────────────────────┘
```

``` sql
SELECT toDecimal32(4.2, 8) AS x, x * x
```

``` text
DB::Exception: Scale is out of bounds.
```

``` sql
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```

``` text
DB::Exception: Decimal math overflow.
```

Les contrôles de débordement entraînent un ralentissement des opérations. S'il est connu que les débordements ne sont pas possibles, il est logique de désactiver les contrôles en utilisant `decimal_check_overflow` paramètre. Lorsque des contrôles sont désactivés et le débordement se produit, le résultat sera faux:

``` sql
SET decimal_check_overflow = 0;
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```

``` text
┌──────────x─┬─multiply(6, toDecimal32(4.2, 8))─┐
│ 4.20000000 │                     -17.74967296 │
└────────────┴──────────────────────────────────┘
```

Les contrôles de débordement se produisent non seulement sur les opérations arithmétiques mais aussi sur la comparaison de valeurs:

``` sql
SELECT toDecimal32(1, 8) < 100
```

``` text
DB::Exception: Can't compare.
```

[Article Original](https://clickhouse.tech/docs/en/data_types/decimal/) <!--hide-->
