---
description: 'Documentation sur les fonctions régulières'
sidebar_label: 'Aperçu'
sidebar_position: 1
slug: /sql-reference/functions/overview
title: 'Fonctions régulières'
doc_type: 'reference'
---

Il existe au moins* deux types de fonctions : les fonctions régulières (appelées simplement « fonctions ») et les fonctions d’agrégation. Ce sont deux concepts complètement différents. Les fonctions régulières se comportent comme si elles étaient appliquées séparément à chaque ligne (pour chaque ligne, le résultat de la fonction ne dépend pas des autres lignes). Les fonctions d’agrégation accumulent un ensemble de valeurs issues de différentes lignes (c’est-à-dire qu’elles dépendent de l’ensemble des lignes).

Dans cette section, nous traitons des fonctions régulières. Pour les fonctions d’agrégation, voir la section « Fonctions d’agrégation ».

:::note
Il existe un troisième type de fonction, auquel appartient la fonction [&#39;arrayJoin&#39;](../functions/array-join.md). On peut également distinguer les [fonctions de table](../table-functions/index.md).
:::

<div id="strong-typing">
  ## Typage strict
</div>

Contrairement au SQL standard, ClickHouse applique un typage strict. En d&#39;autres termes, il n&#39;effectue pas de conversions implicites entre les types. Chaque fonction ne fonctionne qu&#39;avec un ensemble précis de types. Cela signifie que vous devez parfois utiliser des fonctions de conversion de type.

<div id="common-subexpression-elimination">
  ## Élimination des sous-expressions communes
</div>

Toutes les expressions d’une requête qui ont le même AST (le même enregistrement ou le même résultat de l’analyse syntaxique) sont considérées comme ayant des valeurs identiques. Ces expressions sont alors fusionnées et exécutées une seule fois. Les sous-requêtes identiques sont également éliminées de cette façon.

<div id="types-of-results">
  ## Types de résultats
</div>

Toutes les fonctions renvoient une seule valeur en résultat (ni plusieurs, ni aucune). Le type du résultat est généralement défini uniquement par les types des arguments, et non par les valeurs. Les exceptions sont la fonction `tupleElement` (l’opérateur a.N) et la fonction `toFixedString`.

<div id="constants">
  ## Constantes
</div>

Par souci de simplicité, certaines fonctions ne peuvent fonctionner qu’avec des constantes pour certains arguments. Par exemple, l’argument de droite de l’opérateur LIKE doit être une constante.
Presque toutes les fonctions renvoient une constante lorsque leurs arguments sont constants. Font exception les fonctions qui génèrent des nombres aléatoires.
La fonction &#39;now&#39; renvoie des valeurs différentes pour des requêtes exécutées à des moments différents, mais le résultat est considéré comme une constante, puisque la constance n’a d’importance qu’au sein d’une seule requête.
Une expression constante est également considérée comme une constante (par exemple, la partie droite de l’opérateur LIKE peut être construite à partir de plusieurs constantes).

Les fonctions peuvent être implémentées différemment selon que les arguments sont constants ou non (un code différent est exécuté). Mais les résultats obtenus pour une constante et pour une colonne réelle ne contenant que cette même valeur doivent être identiques.

<div id="null-processing">
  ## Traitement des `NULL`
</div>

Les fonctions ont les comportements suivants :

* Si au moins un des arguments de la fonction est `NULL`, le résultat de la fonction est lui aussi `NULL`.
* Comportement spécial défini individuellement dans la description de chaque fonction. Dans le code source de ClickHouse, ces fonctions ont `UseDefaultImplementationForNulls=false`.

<div id="constancy">
  ## Constance
</div>

Les fonctions ne peuvent pas modifier les valeurs de leurs arguments ; toute modification est renvoyée comme résultat. Ainsi, le résultat du calcul de fonctions distinctes ne dépend pas de l&#39;ordre dans lequel elles sont écrites dans la requête.

<div id="higher-order-functions">
  ## Fonctions d’ordre supérieur
</div>

<div id="arrow-operator-and-lambda">
  ### opérateur `->` et fonctions lambda(params, expr)
</div>

Les fonctions d’ordre supérieur ne peuvent accepter que des fonctions lambda comme argument fonctionnel. Pour passer une fonction lambda à une fonction d’ordre supérieur, utilisez l’opérateur `->`. À gauche de la flèche se trouve un paramètre formel, qui peut être n’importe quel identifiant, ou plusieurs paramètres formels — c’est-à-dire n’importe quels identifiants dans un tuple. À droite de la flèche se trouve une expression qui peut utiliser ces paramètres formels ainsi que n’importe quelles colonnes de la table.

Exemples :

```python
x -> 2 * x
str -> str != Referer
```

Une fonction lambda qui accepte plusieurs arguments peut également être passée à une fonction d’ordre supérieur. Dans ce cas, la fonction d’ordre supérieur reçoit plusieurs tableaux de même longueur, un pour chacun de ces arguments.

Pour certaines fonctions, le premier argument (la fonction lambda) peut être omis. Dans ce cas, on suppose une correspondance identique.

<div id="bare-function-names-as-lambdas">
  ### Noms de fonction utilisés comme lambdas
</div>

Au lieu d’écrire une expression lambda complète, vous pouvez passer directement un nom de fonction à une fonction d’ordre supérieur. Le nom de fonction est alors automatiquement converti en une expression lambda équivalente.

Par exemple, les paires suivantes sont équivalentes :

```sql
SELECT arrayMap(negate, [1, 2, 3]);            -- [-1, -2, -3]
SELECT arrayMap(x -> negate(x), [1, 2, 3]);    -- [-1, -2, -3]

SELECT arrayMap(plus, [1, 2, 3], [10, 20, 30]);            -- [11, 22, 33]
SELECT arrayMap((x, y) -> plus(x, y), [1, 2, 3], [10, 20, 30]); -- [11, 22, 33]

SELECT arrayFilter(isNotNull, [1, NULL, 3, NULL, 5]);            -- [1, 3, 5]
SELECT arrayFilter(x -> isNotNull(x), [1, NULL, 3, NULL, 5]);    -- [1, 3, 5]

SELECT arrayFold(plus, [1, 2, 3, 4, 5], toUInt64(0));                      -- 15
SELECT arrayFold((acc, x) -> plus(acc, x), [1, 2, 3, 4, 5], toUInt64(0));  -- 15
```

Cela fonctionne avec les fonctions intégrées, les UDF SQL, les UDF exécutables et les UDF WebAssembly. En cas d&#39;ambiguïté, les noms de colonne et d&#39;alias sont prioritaires sur les noms de fonction.

L&#39;arité de la lambda est déduite de la fonction interne. Par exemple, `arrayMap(plus, ...)` utilise l&#39;arité 2, car `plus` prend deux arguments ; cela fonctionne donc aussi avec des entrées de type Tuple, comme `arrayMap(plus, [(1, 10), (2, 20)])`, où les éléments du tuple sont déballés dans les arguments de la lambda.

Pour les fonctions internes variadiques (comme `concat`, qui accepte un nombre quelconque d&#39;arguments), l&#39;arité de la lambda correspond alors au nombre d&#39;arguments de tableau. C&#39;est correct pour les fonctions d&#39;ordre supérieur comme `arrayMap`, `arrayFilter` et `arrayFold`. Pour les fonctions d&#39;ordre supérieur qui acceptent, en plus des tableaux, des paramètres fixes qui ne sont pas des tableaux — par exemple, `arrayPartialSort(f, limit, arr)` — les noms de fonctions variadiques utilisés seuls peuvent produire une arité incorrecte ; dans ce cas, une lambda explicite est nécessaire.

Les fonctions internes variadiques ne déballent pas non plus automatiquement les entrées de tuple. Par exemple, `arrayMap(concat, [('a', 'b'), ('c', 'd')])` est réécrit en lambda unaire et n&#39;est pas équivalent à `arrayMap((x, y) -> concat(x, y), [('a', 'b'), ('c', 'd')])`. Utilisez une lambda explicite si vous souhaitez déstructurer les éléments du tuple dans un appel variadique.

<div id="user-defined-functions-udfs">
  ## Fonctions définies par l’utilisateur (UDFs)
</div>

ClickHouse prend en charge les fonctions définies par l’utilisateur. Voir [UDFs](../functions/udf.md).