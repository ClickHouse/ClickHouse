---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: Fonction
toc_priority: 32
toc_title: Introduction
---

# Fonction {#functions}

Il y a au moins\* deux types de fonctions - des fonctions régulières (elles sont simplement appelées “functions”) and aggregate functions. These are completely different concepts. Regular functions work as if they are applied to each row separately (for each row, the result of the function doesn't depend on the other rows). Aggregate functions accumulate a set of values from various rows (i.e. they depend on the entire set of rows).

Dans cette section, nous discutons des fonctions classiques. Pour les fonctions d'agrégation, voir la section “Aggregate functions”.

\* - Il existe un troisième type de fonction ‘arrayJoin’ la fonction appartient à; les fonctions de table peuvent également être mentionnées séparément.\*

## Typage Fort {#strong-typing}

Contrairement à SQL standard, ClickHouse a une forte typage. En d'autres termes, il ne fait pas de conversions implicites entre les types. Chaque fonction fonctionne pour un ensemble spécifique de types. Cela signifie que vous devez parfois utiliser des fonctions de conversion de type.

## Élimination Des Sous-Expressions Courantes {#common-subexpression-elimination}

Toutes les expressions d'une requête qui ont le même AST (le même enregistrement ou le même résultat d'analyse syntaxique) sont considérées comme ayant des valeurs identiques. De telles expressions sont concaténées et exécutées une fois. Les sous-requêtes identiques sont également éliminées de cette façon.

## Types de résultats {#types-of-results}

Toutes les fonctions renvoient un seul retour comme résultat (pas plusieurs valeurs, et pas des valeurs nulles). Le type de résultat est généralement défini uniquement par les types d'arguments, pas par les valeurs. Les Exceptions sont la fonction tupleElement (l'opérateur A. N) et la fonction toFixedString.

## Constant {#constants}

Pour simplifier, certaines fonctions ne peuvent fonctionner qu'avec des constantes pour certains arguments. Par exemple, le bon argument de L'opérateur LIKE doit être une constante.
Presque toutes les fonctions renvoient une constante pour des arguments constants. L'exception est les fonctions qui génèrent des nombres aléatoires.
Le ‘now’ function renvoie des valeurs différentes pour les requêtes qui ont été exécutées à des moments différents, mais le résultat est considéré comme une constante, car la constance n'est importante que dans une seule requête.
Une expression constante est également considérée comme une constante (par exemple, la moitié droite de L'opérateur LIKE peut être construite à partir de plusieurs constantes).

Les fonctions peuvent être implémentées de différentes manières pour des arguments constants et non constants (un code différent est exécuté). Mais les résultats pour une constante et pour une colonne vraie Ne contenant que la même valeur doivent correspondre les uns aux autres.

## Le Traitement NULL {#null-processing}

Les fonctions ont les comportements suivants:

-   Si au moins l'un des arguments de la fonction est `NULL` le résultat de la fonction est également `NULL`.
-   Comportement spécial spécifié individuellement dans la description de chaque fonction. Dans le code source de ClickHouse, ces fonctions ont `UseDefaultImplementationForNulls=false`.

## Constance {#constancy}

Functions can't change the values of their arguments – any changes are returned as the result. Thus, the result of calculating separate functions does not depend on the order in which the functions are written in the query.

## Erreur De Manipulation {#error-handling}

Certaines fonctions peuvent lancer une exception si les données ne sont pas valides. Dans ce cas, la requête est annulée et un message d'erreur est retourné au client. Pour le traitement distribué, lorsqu'une exception se produit sur l'un des serveurs, les autres serveurs aussi tenté d'interrompre la requête.

## Évaluation des Expressions D'Argument {#evaluation-of-argument-expressions}

Dans presque tous les langages de programmation, l'un des arguments peut pas être évalué pour certains opérateurs. Ce sont généralement les opérateurs `&&`, `||`, et `?:`.
Mais dans ClickHouse, les arguments des fonctions (opérateurs) sont toujours évalués. En effet, des parties entières de colonnes sont évaluées à la fois, au lieu de calculer chaque ligne séparément.

## Exécution de fonctions pour le traitement de requêtes distribuées {#performing-functions-for-distributed-query-processing}

Pour le traitement de requête distribué, autant d'étapes de traitement de requête que possible sont effectuées sur des serveurs distants, et le reste des étapes (fusion des résultats intermédiaires et tout ce qui suit) sont effectuées sur le serveur demandeur.

Cela signifie que les fonctions peuvent être effectuées sur différents serveurs.
Par exemple, dans la requête `SELECT f(sum(g(x))) FROM distributed_table GROUP BY h(y),`

-   si un `distributed_table` a au moins deux fragments, les fonctions ‘g’ et ‘h’ sont effectuées sur des serveurs distants, et la fonction ‘f’ est effectuée sur le serveur demandeur.
-   si un `distributed_table` a un seul fragment, tous les ‘f’, ‘g’, et ‘h’ les fonctions sont exécutées sur le serveur de ce fragment.

Le résultat d'une fonction habituellement ne dépendent pas le serveur sur lequel elle est exécutée. Cependant, parfois c'est important.
Par exemple, les fonctions qui fonctionnent avec des dictionnaires utilisent le dictionnaire qui existe sur le serveur sur lequel elles s'exécutent.
Un autre exemple est l' `hostName` fonction, qui renvoie le nom du serveur sur lequel il s'exécute afin de `GROUP BY` par les serveurs dans un `SELECT` requête.

Si une fonction dans une requête est effectuée sur le demandeur serveur, mais vous devez l'exécuter sur des serveurs distants, vous pouvez l'envelopper dans un ‘any’ fonction d'agrégation ou l'ajouter à une clé dans `GROUP BY`.

[Article Original](https://clickhouse.tech/docs/en/query_language/functions/) <!--hide-->
