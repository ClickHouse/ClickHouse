---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: Comparaison
---

# Fonctions De Comparaison {#comparison-functions}

Les fonctions de comparaison renvoient toujours 0 ou 1 (Uint8).

Les types suivants peuvent être comparés:

-   nombre
-   cordes et cordes fixes
-   date
-   dates avec heures

au sein de chaque groupe, mais pas entre différents groupes.

Par exemple, vous ne pouvez pas comparer une date avec une chaîne. Vous devez utiliser une fonction pour convertir la chaîne en une date, ou vice versa.

Les chaînes sont comparées par octets. Une courte chaîne est plus petite que toutes les chaînes qui commencent par elle et qui contiennent au moins un caractère de plus.

## égal, A = B et a = = b opérateur {#function-equals}

## notEquals, a ! opérateur= b et a \<\> b {#function-notequals}

## moins, opérateur \<  {#function-less}

## de plus, \> opérateur {#function-greater}

## lessOrEquals, \< = opérateur {#function-lessorequals}

## greaterOrEquals, \> = opérateur {#function-greaterorequals}

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/comparison_functions/) <!--hide-->
