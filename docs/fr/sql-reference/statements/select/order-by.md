---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# Clause ORDER BY {#select-order-by}

Le `ORDER BY` clause contient une liste des expressions, qui peuvent être attribuées avec `DESC` (décroissant) ou `ASC` modificateur (ascendant) qui détermine la direction de tri. Si la direction n'est pas spécifié, `ASC` est supposé, donc il est généralement omis. La direction de tri s'applique à une seule expression, pas à la liste entière. Exemple: `ORDER BY Visits DESC, SearchPhrase`

Les lignes qui ont des valeurs identiques pour la liste des expressions de tri sont sorties dans un ordre arbitraire, qui peut également être non déterministe (différent à chaque fois).
Si la clause ORDER BY est omise, l'ordre des lignes est également indéfini et peut également être non déterministe.

## Tri des valeurs spéciales {#sorting-of-special-values}

Il existe deux approches pour `NaN` et `NULL` ordre de tri:

-   Par défaut ou avec le `NULLS LAST` modificateur: d'abord les valeurs, puis `NaN`, puis `NULL`.
-   Avec l' `NULLS FIRST` modificateur: première `NULL`, puis `NaN` puis d'autres valeurs.

### Exemple {#example}

Pour la table

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    2 │
│ 1 │  nan │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │  nan │
│ 7 │ ᴺᵁᴸᴸ │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

Exécuter la requête `SELECT * FROM t_null_nan ORDER BY y NULLS FIRST` obtenir:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 7 │ ᴺᵁᴸᴸ │
│ 1 │  nan │
│ 6 │  nan │
│ 2 │    2 │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

Lorsque les nombres à virgule flottante sont triés, les Nan sont séparés des autres valeurs. Quel que soit l'ordre de tri, NaNs viennent à la fin. En d'autres termes, pour le Tri ascendant, ils sont placés comme s'ils étaient plus grands que tous les autres nombres, tandis que pour le Tri descendant, ils sont placés comme s'ils étaient plus petits que les autres.

## Classement De Soutien {#collation-support}

Pour le tri par valeurs de chaîne, vous pouvez spécifier le classement (comparaison). Exemple: `ORDER BY SearchPhrase COLLATE 'tr'` - pour le tri par mot-clé dans l'ordre croissant, en utilisant l'alphabet turc, insensible à la casse, en supposant que les chaînes sont encodées en UTF-8. COLLATE peut être spécifié ou non pour chaque expression dans L'ordre par indépendamment. Si ASC ou DESC est spécifié, COLLATE est spécifié après. Lors de L'utilisation de COLLATE, le tri est toujours insensible à la casse.

Nous recommandons uniquement D'utiliser COLLATE pour le tri final d'un petit nombre de lignes, car le tri avec COLLATE est moins efficace que le tri normal par octets.

## Détails De Mise En Œuvre {#implementation-details}

Moins de RAM est utilisé si un assez petit [LIMIT](limit.md) est précisée en plus `ORDER BY`. Sinon, la quantité de mémoire dépensée est proportionnelle au volume de données à trier. Pour le traitement des requêtes distribuées, si [GROUP BY](group-by.md) est omis, le tri est partiellement effectué sur les serveurs distants et les résultats sont fusionnés Sur le serveur demandeur. Cela signifie que pour le tri distribué, le volume de données à trier peut être supérieur à la quantité de mémoire sur un seul serveur.

S'il N'y a pas assez de RAM, il est possible d'effectuer un tri dans la mémoire externe (création de fichiers temporaires sur un disque). Utilisez le paramètre `max_bytes_before_external_sort` pour ce but. S'il est défini sur 0 (par défaut), le tri externe est désactivé. Si elle est activée, lorsque le volume de données à trier atteint le nombre spécifié d'octets, les données collectées sont triés et déposés dans un fichier temporaire. Une fois toutes les données lues, tous les fichiers triés sont fusionnés et les résultats sont générés. Les fichiers sont écrits dans le `/var/lib/clickhouse/tmp/` dans la configuration (par défaut, mais vous pouvez `tmp_path` paramètre pour modifier ce paramètre).

L'exécution d'une requête peut utiliser plus de mémoire que `max_bytes_before_external_sort`. Pour cette raison, ce paramètre doit avoir une valeur significativement inférieure à `max_memory_usage`. Par exemple, si votre serveur dispose de 128 Go de RAM et que vous devez exécuter une seule requête, définissez `max_memory_usage` à 100 Go, et `max_bytes_before_external_sort` à 80 Go.

Le tri externe fonctionne beaucoup moins efficacement que le tri dans la RAM.
