---
machine_translated: true
---

# Fonctions de comparaison {#comparison-functions}

Les fonctions de comparaison renvoient toujours 0 ou 1 (Uint8).

Les types suivants peuvent être comparés:

-   nombre
-   cordes et cordes fixes
-   date
-   dates avec heures

au sein de chaque groupe, mais pas entre différents groupes.

Par exemple, vous ne pouvez pas comparer une date avec une chaîne. Vous devez utiliser une fonction pour convertir la chaîne en une date, ou vice versa.

Les chaînes sont comparées par octets. Une courte chaîne est plus petite que toutes les chaînes qui commencent par elle et qui contiennent au moins un caractère de plus.

Note. Jusqu'à la version 1.1.54134, les numéros signés et non signés étaient comparés de la même manière qu'en C++. En d'autres termes, vous pourriez obtenir un résultat incorrect dans des cas comme SELECT 9223372036854775807 \> -1. Ce comportement a changé dans la version 1.1.54134 et est maintenant mathématiquement correct.

## égal, A = B et a = = b opérateur {#function-equals}

## notEquals, a ! opérateur= b et a `<>` b {#function-notequals}

## peu, `< operator` {#function-less}

## grand, `> operator` {#function-greater}

## lessOrEquals, `<= operator` {#function-lessorequals}

## greaterOrEquals, `>= operator` {#function-greaterorequals}

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/comparison_functions/) <!--hide-->
