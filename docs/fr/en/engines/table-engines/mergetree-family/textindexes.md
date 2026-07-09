---
description: 'Trouvez rapidement des termes de recherche dans du texte.'
keywords: ['recherche en texte intégral', 'index de texte', 'index', 'indices']
sidebar_label: 'Recherche en texte intégral avec des index de texte'
slug: /engines/table-engines/mergetree-family/textindexes
title: 'Recherche en texte intégral avec des index de texte'
doc_type: 'reference'
---

Les index de texte (également appelés [index inversés](https://en.wikipedia.org/wiki/Inverted_index)) permettent d’effectuer rapidement des recherches en texte intégral dans des données textuelles.
Un index de texte stocke une correspondance entre les tokens et les numéros de ligne qui contiennent chaque token.
Les tokens sont générés par un processus appelé tokenisation.
Par exemple, le tokenizer par défaut de ClickHouse convertit la phrase anglaise &quot;The cat likes mice.&quot; en tokens [&quot;The&quot;, &quot;cat&quot;, &quot;likes&quot;, &quot;mice&quot;].

À titre d’exemple, supposons une table avec une seule colonne et trois lignes

```result
1: The cat likes mice.
2: Mice are afraid of dogs.
3: I have two dogs and a cat.
```

Les tokens correspondants sont :

```result
1: The, cat, likes, mice
2: Mice, are, afraid, of, dogs
3: I, have, two, dogs, and, a, cat
```

Nous effectuons généralement des recherches sans tenir compte de la casse, nous mettons donc les tokens en minuscules :

```result
1: the, cat, likes, mice
2: mice, are, afraid, of, dogs
3: i, have, two, dogs, and, a, cat
```

Nous supprimerons également les mots vides tels que &quot;I&quot;, &quot;the&quot; et &quot;and&quot;, car ils apparaissent dans presque chaque ligne :

```result
1: cat, likes, mice
2: mice, afraid, dogs
3: have, two, dogs, cat
```

Un index de texte contient alors (du point de vue conceptuel) les informations suivantes :

```result
afraid : [2]
cat    : [1, 3]
dogs   : [2, 3]
have   : [3]
likes  : [1]
mice   : [1]
two    : [3]
```

Pour un token de recherche donné, cette structure d’index permet de retrouver rapidement toutes les lignes correspondantes.

<div id="creating-a-text-index">
  ## Création d’un index de texte
</div>

Les index de texte sont généralement disponibles (GA) dans ClickHouse à partir de la version 26.2.
Dans ces versions, aucun paramètre particulier n’a besoin d’être configuré pour utiliser l’index de texte.
Nous recommandons vivement d’utiliser ClickHouse en version &gt;= 26.2 pour les cas d’usage en production.

:::note
Les index de texte peuvent être utilisés avec n’importe quelle version de ClickHouse &gt;= 26.2, quel que soit le paramètre de [compatibilité](../../../operations/settings/settings#compatibility).
:::

Pour créer un index de texte, utilisez la syntaxe suivante :

```sql title="Query"
CREATE TABLE table
(
    key UInt64,
    str String,
    INDEX text_idx str TYPE text(
                                -- Mandatory parameters:
                                tokenizer = splitByNonAlpha
                                            | splitByString[(S)]
                                            | asciiCJK
                                            | ngrams[(N)]
                                            | sparseGrams[(min_length[, max_length[, min_cutoff_length]])]
                                            | array
                                -- Optional parameters:
                                [, preprocessor = expression(str)]
                                [, postprocessor = expression(str)]
                                [, positions = 0 | 1 ] -- experimental
                                -- Optional advanced parameters:
                                [, dictionary_block_size = D]
                                [, dictionary_block_frontcoding_compression = B]
                                [, posting_list_block_size = C]
                                [, posting_list_codec = 'none' | 'bitpacking' ]
                            )
)
ENGINE = MergeTree
ORDER BY key
```

Les index de texte peuvent être définis sur des colonnes de ces types :

* [String](/fr/sql-reference/data-types/string.md) et [FixedString](/fr/sql-reference/data-types/fixedstring.md),
* [Array(String)](/fr/sql-reference/data-types/array.md) et [Array(FixedString)](/fr/sql-reference/data-types/array.md),
* [Map](/fr/sql-reference/data-types/map.md) (à l’aide des fonctions [mapKeys](/fr/sql-reference/functions/tuple-map-functions.md/#mapKeys) et [mapValues](/fr/sql-reference/functions/tuple-map-functions.md/#mapValues)), et
* [JSON](/fr/sql-reference/data-types/newjson.md) (à l’aide des fonctions [JSONAllPaths](/fr/sql-reference/functions/json-functions.md/#JSONAllPaths) et [`JSONAllValues`](/fr/sql-reference/functions/json-functions.md#JSONAllValues)).

Les colonnes de type [Nullable(T)](/fr/sql-reference/data-types/nullable.md) et [LowCardinality()](/fr/sql-reference/data-types/lowcardinality.md) sont également prises en charge, y compris `Array(Nullable(String or FixedString))`.

Vous pouvez également ajouter un index de texte à une table existante :

```sql title="Query"
ALTER TABLE table
    ADD INDEX text_idx str TYPE text(
                                -- Mandatory parameters:
                                tokenizer = splitByNonAlpha
                                            | splitByString[(S)]
                                            | asciiCJK
                                            | ngrams[(N)]
                                            | sparseGrams[(min_length[, max_length[, min_cutoff_length]])]
                                            | array
                                -- Optional parameters:
                                [, preprocessor = expression(str)]
                                [, postprocessor = expression(str)]
                                [, positions = 0 | 1 ] -- experimental
                                -- Optional advanced parameters:
                                [, dictionary_block_size = D]
                                [, dictionary_block_frontcoding_compression = B]
                                [, posting_list_block_size = C]
                                [, posting_list_codec = 'none' | 'bitpacking' ]
                            )

```

Si vous ajoutez un index à une table existante, nous vous recommandons de matérialiser l’index pour les parts de la table existantes (sinon, la recherche sur les parts sans index basculera vers de lents balayages exhaustifs).

```sql title="Query"
ALTER TABLE table MATERIALIZE INDEX text_idx SETTINGS mutations_sync = 2;
```

Pour supprimer un index de texte intégral, exécutez

```sql title="Query"
ALTER TABLE table DROP INDEX text_idx;
```

**Argument `tokenizer` (obligatoire)**. L’argument `tokenizer` spécifie le tokenizer :

* `splitByNonAlpha` découpe les chaînes sur les caractères ASCII non alphanumériques (voir la fonction [splitByNonAlpha](/fr/sql-reference/functions/splitting-merging-functions.md/#splitByNonAlpha)).
* `splitByString(S)` découpe les chaînes à l&#39;aide de certaines chaînes séparatrices `S` définies par l&#39;utilisateur (voir la fonction [splitByString](/fr/sql-reference/functions/splitting-merging-functions.md/#splitByString)).
  Les séparateurs peuvent être spécifiés à l&#39;aide d&#39;un paramètre facultatif, par exemple `tokenizer = splitByString([', ', '; ', '\n', '\\'])`.
  Notez que chaque chaîne peut être constituée de plusieurs caractères (`', '` dans l&#39;exemple).
  La liste de séparateurs par défaut, si elle n&#39;est pas explicitement spécifiée (par exemple `tokenizer = splitByString`), est un espace unique `[' ']`.
* `asciiCJK` découpe les chaînes en tokens à l&#39;aide des règles Unicode de délimitation des mots (similaires à [Unicode Text Segmentation (UAX #29)](https://unicode.org/reports/tr29/)). Les caractères ASCII alphanumériques et les traits de soulignement forment des tokens avec des connecteurs (ASCII `:` pour les lettres, `.` et `'` pour les caractères de même type). Les caractères Unicode non ASCII, y compris les caractères [CJK](https://en.wikipedia.org/wiki/CJK_characters), deviennent des tokens d&#39;un seul caractère.
* `ngrams(N)` découpe les chaînes en `N`-grammes de taille égale (voir la fonction [ngrams](/fr/sql-reference/functions/splitting-merging-functions.md/#ngrams)).
  La longueur des n-grammes peut être spécifiée à l&#39;aide d&#39;un paramètre entier facultatif compris entre 1 et 8, par exemple `tokenizer = ngrams(3)`.
  La taille des n-grammes par défaut, si elle n&#39;est pas explicitement spécifiée (par exemple `tokenizer = ngrams`), est 3.
* `sparseGrams(min_length, max_length, min_cutoff_length)` découpe les chaînes en n-grammes de longueur variable d&#39;au moins `min_length` et d&#39;au plus `max_length` caractères (bornes incluses) (voir la fonction [sparseGrams](/fr/sql-reference/functions/string-functions#sparseGrams)).
  Sauf indication explicite, `min_length` et `max_length` valent par défaut 3 et 100.
  Si le paramètre `min_cutoff_length` est fourni, seuls les n-grammes dont la longueur est supérieure ou égale à `min_cutoff_length` sont renvoyés.
  Par rapport à `ngrams(N)`, le tokenizer `sparseGrams` produit des N-grammes de longueur variable, ce qui permet une représentation plus souple du texte d&#39;origine.
  Par exemple, `tokenizer = sparseGrams(3, 5, 4)` génère en interne des 3-, 4- et 5-grammes à partir de la chaîne d&#39;entrée, mais seuls les 4- et 5-grammes sont renvoyés.
* `array` n&#39;effectue aucune tokenization, c.-à-d. que chaque valeur de ligne constitue un token (voir la fonction [array](/fr/sql-reference/functions/array-functions.md/#array)).

Tous les tokenizers disponibles sont répertoriés dans [system.tokenizers](../../../operations/system-tables/tokenizers.md).

:::note
Le tokenizer `splitByString` applique les séparateurs de découpe de gauche à droite.
Cela peut créer des ambiguïtés.
Par exemple, les chaînes séparatrices `['%21', '%']` feront que `%21abc` sera découpé en `['abc']`, alors qu&#39;en inversant l&#39;ordre des deux chaînes séparatrices en `['%', '%21']`, la sortie sera `['21abc']`.
Dans la plupart des cas, vous voudrez que la correspondance privilégie d&#39;abord les séparateurs les plus longs.
Cela peut généralement être obtenu en passant les chaînes séparatrices par ordre décroissant de longueur.
Si les chaînes séparatrices forment un [code préfixe](https://en.wikipedia.org/wiki/Prefix_code), elles peuvent être passées dans n&#39;importe quel ordre.
:::

Pour comprendre comment un tokenizer découpe la chaîne d&#39;entrée, vous pouvez utiliser les fonctions [tokens](/fr/sql-reference/functions/splitting-merging-functions.md/#tokens) et [tokensForLikePattern](/fr/sql-reference/functions/splitting-merging-functions.md/#tokensForLikePattern) :

Exemple :

```sql title="Query"
SELECT tokens('abc def', 'ngrams', 3);
```

```result title="Response"
['abc','bc ','c d',' de','def']
```

*Utilisation d’entrées non ASCII.*
Les index de texte peuvent être créés à partir de données textuelles dans n’importe quelle langue et avec n’importe quel jeu de caractères.
Pour le texte non ASCII, le tokenizer `asciiCJK` est recommandé, car il gère correctement les limites de mots Unicode, y compris pour les caractères CJK.
:::

**Argument de préprocesseur (facultatif)**. Le préprocesseur désigne une expression appliquée à la chaîne d’entrée avant la tokenisation.

Les cas d’utilisation typiques de l’argument de préprocesseur incluent

1. Mise en minuscules/majuscules, ou normalisation de la casse pour permettre une correspondance insensible à la casse, par exemple [lower](/fr/sql-reference/functions/string-functions.md/#lower), [lowerUTF8](/fr/sql-reference/functions/string-functions.md/#lowerUTF8), [caseFoldUTF8](/fr/sql-reference/functions/string-functions.md/#caseFoldUTF8).
2. Normalisation UTF-8, par ex. [normalizeUTF8NFC](/fr/sql-reference/functions/string-functions.md/#normalizeUTF8NFC), [normalizeUTF8NFD](/fr/sql-reference/functions/string-functions.md/#normalizeUTF8NFD), [normalizeUTF8NFKC](/fr/sql-reference/functions/string-functions.md/#normalizeUTF8NFKC), [normalizeUTF8NFKD](/fr/sql-reference/functions/string-functions.md/#normalizeUTF8NFKD), [normalizeUTF8NFKCCasefold](/fr/sql-reference/functions/string-functions.md/#normalizeUTF8NFKCCasefold), [toValidUTF8](/fr/sql-reference/functions/string-functions.md/#toValidUTF8).
3. Suppression ou transformation de caractères ou de sous-chaînes indésirables, comme les accents, par ex. [extractTextFromHTML](/fr/sql-reference/functions/string-functions.md/#extractTextFromHTML), [substring](/fr/sql-reference/functions/string-functions.md/#substring), [idnaEncode](/fr/sql-reference/functions/string-functions.md/#idnaEncode), [translate](/fr/sql-reference/functions/string-replace-functions.md/#translate), [removeDiacriticsUTF8](/fr/sql-reference/functions/string-functions.md/#removeDiacriticsUTF8).

L&#39;expression de préprocesseur doit transformer une valeur d’entrée de type [String](/fr/sql-reference/data-types/string.md) ou [FixedString](/fr/sql-reference/data-types/fixedstring.md) en une valeur du même type.
Si l&#39;index de texte a été créé sur une colonne de type `Nullable(T)` ou `LowCardinality(T)`, l&#39;expression de préprocesseur doit alors accepter des valeurs nullable ou à faible cardinalité (c.-à-d. ne pas lever d&#39;exception).

Exemples :

* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(col))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = substringIndex(col, '\n', 1))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(extractTextFromHTML(col)))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = removeDiacriticsUTF8(caseFoldUTF8(col)))`

De plus, l&#39;expression de préprocesseur ne doit faire référence qu&#39;à la colonne ou à l&#39;expression sur laquelle l&#39;index de texte est défini.

Exemples :

* `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = upper(lower(col)))`
* `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(lower(col), lower(col)))`
* Non autorisé : `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(col, col))`

L&#39;utilisation de fonctions non déterministes n&#39;est pas autorisée.

:::note
Les préprocesseurs sont en principe équivalents à l&#39;encapsulation de la colonne ou de l&#39;expression indexée dans l&#39;expression de préprocesseur.
Par exemple, le préprocesseur `lower` dans `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(col))` peut être émulé par `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha')`.
Cette dernière forme a l&#39;inconvénient que le préprocesseur émulé n&#39;est appliqué que s&#39;il correspond à la condition de filtre dans la clause WHERE.
Par exemple, `WHERE hasAllTokens(lower(col), [...])` correspond, tandis que `WHERE hasAllTokens(col, [...])` ne correspond pas.
Pour une expérience utilisateur optimale, nous recommandons donc d&#39;utiliser des expressions de préprocesseur.
:::

Les fonctions [hasToken](/fr/sql-reference/functions/string-search-functions.md/#hasToken), [hasAllTokens](/fr/sql-reference/functions/string-search-functions.md/#hasAllTokens), [hasAnyTokens](/fr/sql-reference/functions/string-search-functions.md/#hasAnyTokens) et [hasPhrase](/fr/sql-reference/functions/string-search-functions.md/#hasPhrase) utilisent le préprocesseur pour transformer d&#39;abord le terme de recherche avant de le découper en tokens.
Notez que, comme le préprocesseur n&#39;est appliqué que sur le chemin d&#39;exécution de l&#39;index de texte, les résultats de ces fonctions peuvent différer entre les requêtes qui utilisent l&#39;index de texte et celles qui ne l&#39;utilisent pas (par ex. `SETTINGS use_skip_indexes = 0`).

Par exemple,

```sql title="Query"
CREATE TABLE table
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(str))
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT count() FROM table WHERE hasToken(str, 'Foo');
```

est équivalent à :

```sql title="Query"
CREATE TABLE table
(
    str String,
    INDEX idx lower(str) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT count() FROM table WHERE hasToken(str, lower('Foo'));
```

Dans ce cas, l’expression de prétraitement transforme les éléments du tableau un par un.

Exemple :

```sql title="Query"
CREATE TABLE table
(
    arr Array(String),
    INDEX idx arr TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(arr))

    -- This is not legal:
    INDEX idx_illegal arr TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = arraySort(arr))
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT count() FROM tab WHERE hasAllTokens(arr, 'foo');
```

Pour définir un préprocesseur dans un index de texte créé sur des colonnes de type [Map](/fr/sql-reference/data-types/map.md), les utilisateurs doivent déterminer si l’index est
construit sur les clés ou sur les valeurs de la map.

Exemple :

```sql title="Query"
CREATE TABLE table
(
    map Map(String, String),
    INDEX idx mapKeys(map)  TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(mapKeys(map)))
)
ENGINE = MergeTree
ORDER BY tuple();

SELECT count() FROM tab WHERE hasAllTokens(mapKeys(map), 'foo');
```

**Argument postprocessor (facultatif)**. Le postprocessor désigne une expression appliquée à chaque token de sortie après la tokenisation.

Contrairement au préprocesseur, qui transforme toute la chaîne d’entrée avant que le tokenizer ne la découpe en tokens, le postprocessor agit directement sur les tokens, un par un.
C’est l’endroit idéal pour les transformations qui s’appliquent par nature au niveau du token.

Les cas d’usage typiques de l’argument postprocessor incluent :

1. **Filtrage des stop words (tokens extrêmement fréquents)**. Les tokens très courants tels que &quot;the&quot;, &quot;a&quot; et &quot;is&quot; ont peu d’intérêt pour la recherche et alourdissent l’index.
   Vous pouvez utiliser le postprocessor pour les éliminer en les convertissant en tokens vides — les tokens vides sont ignorés, c.-à-d. qu’ils ne sont pas ajoutés à l’index.
   Exemple : `if(str IN ('the', 'a', 'an', 'of', 'in', 'is', 'it'), '', str)`
2. **Suppression des timestamps**. Les lignes de log commencent souvent par un timestamp structuré ou en contiennent un, tel que `2024-01-15T10:23:45`.
   L’indexing des tokens de timestamp gonfle l’index avec des chaînes qui n’apportent aucune pertinence à la recherche.
   Il existe deux approches complémentaires pour ignorer les timestamps :
   * **Approche postprocessor** : utilisez le tokenizer `splitByString` (découpage sur les espaces) afin que le timestamp entier devienne un seul token, puis utilisez `parseDateTimeOrNull` pour le détecter et le supprimer.
     Exemple : `if(isNull(parseDateTimeOrNull(str, '%Y-%m-%dT%H:%i:%S')), str, '')`
     Pour les timestamps avec des offsets de timezone ou des fractions de seconde, utilisez `parseDateTimeBestEffortOrNull(str)` sans format string explicite.
   * **Approche préprocesseur** : supprimez le timestamp de la ligne de log complète *avant* la tokenisation à l’aide d’une regular expression.
     Exemple : `replaceRegexpAll(str, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} ', '')`
     Cela fonctionne avec n’importe quel tokenizer et est plus efficace, puisque les caractères du timestamp ne sont jamais convertis en tokens.
     Les deux approches peuvent être combinées : le préprocesseur supprime le timestamp tandis que le postprocessor normalise ou filtre les tokens restants (par ex., passage en minuscules + suppression des mots de sévérité comme `ERROR` ou `INFO`).
3. **Stemming**. Associer chaque token à sa racine améliore le rappel en recherche en faisant correspondre des variantes morphologiques qui partagent la même racine.
   Par exemple, avec le stemming anglais, &quot;running&quot;, &quot;runs&quot; et &quot;run&quot; sont tous ramenés à &quot;run&quot;, de sorte qu’une query sur l’une de ces variantes correspond à toutes les autres.
   ClickHouse fournit une fonction intégrée [stem](/fr/sql-reference/functions/string-functions.md/#stem) pour plusieurs langues.
   Exemple : `stem(str, 'en')`
4. **Normalisation de la casse**. Passage des tokens en minuscules ou en majuscules pour permettre une correspondance case-insensitive, par ex. [lower](/fr/sql-reference/functions/string-functions.md/#lower), [lowerUTF8](/fr/sql-reference/functions/string-functions.md/#lowerUTF8).
   Pour la conversion en minuscules et en majuscules, nous recommandons d’utiliser un préprocesseur plutôt qu’un postprocessor.

L’expression de postprocessor transforme des tokens de type [String](/fr/sql-reference/data-types/string.md) en tokens du même type.
De plus, l’expression de postprocessor ne doit référencer que la colonne ou l’expression sur laquelle le text index est défini.
Lorsque la colonne est de type `Array(String)`, le postprocessor continue d’agir sur chaque token individuellement, comme sur de simples valeurs `String`.

L&#39;utilisation de fonctions non déterministes est interdite.

Le postprocessor est appliqué à chaque token généré lors de la construction de l&#39;index (pour le tokenizer `array`, chaque élément du tableau est un token). Au moment de la requête, le comportement dépend de la fonction :

* Pour `hasToken`, `hasAllTokens`, `hasAnyTokens` et `hasPhrase` (avec n&#39;importe quel tokenizer pris en charge) : le postprocessor est appliqué à la fois aux tokens du haystack et au needle de recherche, ce qui permet une correspondance entièrement normalisée (par ex., une recherche insensible à la casse). Pour `hasPhrase`, les tokens post-traités sont positionnés de manière dense, de sorte qu&#39;un token supprimé par le postprocessor ne laisse aucun écart de position et que la phrase continue à correspondre malgré cela — par ex. avec un postprocessor de stop words qui supprime `the`, `hasPhrase(col, 'see cat')` correspond à un document `see the cat`.
* Pour toutes les autres fonctions (`=`, `IN`, `has`, `hasAny`, `hasAll`, `mapContains*`) : seul le needle de recherche est post-traité pour la recherche avec indication d&#39;index ; le prédicat au niveau des lignes continue à être comparé aux valeurs d&#39;origine de la colonne.

Exemples :

* Supprimez les stop words à l&#39;aide d&#39;une postprocessor expression :

```sql
CREATE TABLE table
(
    str String,
    INDEX idx(str) TYPE text(
        tokenizer = 'splitByNonAlpha',
        postprocessor = if(str IN ('the', 'a', 'an', 'of', 'in', 'is', 'it'), '', str)
    )
)
ENGINE = MergeTree
ORDER BY tuple();
```

* Supprimez les horodatages à l’aide d’une expression de post-traitement :

```sql
-- Log lines: '2024-01-15T10:23:45 ERROR connection failed'
-- The splitByString tokenizer (default: whitespace) keeps the full timestamp as one token.
-- parseDateTimeOrNull detects and drops it; non-timestamp words are kept.
CREATE TABLE logs
(
    id   UInt64,
    line String,
    INDEX idx(line) TYPE text(
        tokenizer    = 'splitByString',
        postprocessor = if(isNull(parseDateTimeOrNull(line, '%Y-%m-%dT%H:%i:%S')), line, '')
    )
)
ENGINE = MergeTree ORDER BY id;

-- Only message-level words are indexed; timestamp tokens are not stored.
SELECT count() FROM logs WHERE hasAllTokens(line, ['ERROR']);       -- fast index lookup
SELECT count() FROM logs WHERE hasAllTokens(line, ['2024-01-15T10:23:45']);  -- returns 0: token was never indexed
```

* Supprimez les horodatages à l’aide d’une expression de préprocesseur :

```sql
-- The preprocessor strips the ISO timestamp prefix before tokenization.
-- Any tokenizer can be used; timestamp characters are never seen by the tokenizer.
CREATE TABLE logs
(
    id   UInt64,
    line String,
    INDEX idx(line) TYPE text(
        tokenizer   = 'splitByNonAlpha',
        preprocessor = replaceRegexpAll(line, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} ', '')
    )
)
ENGINE = MergeTree ORDER BY id;
```

* Supprimez les horodatages à l’aide d’une expression combinant préprocesseur et postprocessor :

```sql
-- Preprocessor strips the timestamp, then lowercases the remainder.
-- Postprocessor drops the severity word (error, info, warn, debug) after tokenization.
-- Result: only substantive message words are stored in the index.
CREATE TABLE logs
(
    id   UInt64,
    line String,
    INDEX idx(line) TYPE text(
        tokenizer    = 'splitByNonAlpha',
        preprocessor = lower(replaceRegexpAll(line, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} ', '')),
        postprocessor = if(line IN ('error', 'info', 'warn', 'warning', 'debug', 'critical'), '', line)
    )
)
ENGINE = MergeTree ORDER BY id;

-- Example log line: '2024-01-15T10:23:45 ERROR connection failed'
-- After preprocessor:  'error connection failed'
-- After tokenization:  ['error', 'connection', 'failed']
-- After postprocessor: ['connection', 'failed']   ← 'error' dropped as severity word
SELECT count() FROM logs WHERE hasAllTokens(line, ['connection']);
```

* Racinisez les tokens à l’aide d’une expression de post-traitement :

```sql
CREATE TABLE table
(
    str String,
    INDEX idx(str) TYPE text(
        tokenizer = 'splitByNonAlpha',
        postprocessor = stem(str, 'en')
    )
)
ENGINE = MergeTree
ORDER BY tuple();

-- The query token 'running' is stemmed to 'run' before the lookup,
-- matching rows that contain 'run', 'runs', 'ran', 'running', etc.
SELECT count() FROM table WHERE hasAllTokens(str, ['running']);
```

**Prise en charge des fonctions**.

Pour les prédicats qui consultent l’index de texte, le préprocesseur et le postprocesseur sont appliqués à la valeur recherchée avant la vérification au niveau du granule, afin que la recherche dans l’index utilise les mêmes tokens que ceux stockés lors de la création de l’index.
Pour la plupart des fonctions (`=`, `IN`, `startsWith`, `endsWith`, `LIKE`, `mapContains*`), l’index de texte sert uniquement à ignorer les blocs de données non pertinents ; ClickHouse vérifie ensuite chaque ligne conservée à l’aide du prédicat d’origine sur les données de colonne d’origine.
Pour les fonctions de recherche de tokens (`hasToken`, `hasAllTokens`, `hasAnyTokens`), l’index de texte constitue le principal chemin d’évaluation : ClickHouse normalise le needle à l’aide du même préprocesseur, tokenizer et postprocesseur que ceux appliqués lors de la création de l’index, et utilise cette forme normalisée pour les parts de table indexées comme non indexées. Avec un postprocesseur, les tokens du haystack sont également normalisés au moment de la requête (pour n’importe quel tokenizer, pas seulement `array`), de sorte que les deux côtés de la comparaison sont transformés de manière cohérente et que le résultat ne dépend pas du fait que l’index soit lu directement (paramètre `query_plan_direct_read_from_text_index`) ni du fait qu’une part donnée dispose d’un index matérialisé — par exemple, pour activer une correspondance insensible à la casse pour `hasAllTokens(col, ['FOO'])` avec un postprocesseur `lower`.
Sans `positions`, `hasPhrase` utilise l’index uniquement comme indication et vérifie chaque ligne conservée avec le prédicat d’origine ; un postprocesseur normalise en outre la phrase et les tokens du haystack de la même manière, de sorte que le résultat est indépendant du chemin de lecture, et les tokens que le postprocesseur supprime ne rompent pas l’adjacence de la phrase. Avec `positions = 1`, `hasPhrase` utilise des lectures directes exactes (tout en appliquant le postprocesseur, le cas échéant).
Les tokens de recherche que le postprocesseur transforme en chaîne vide sont ignorés, c’est-à-dire traités comme absents de la phrase de recherche.

| Fonction                                                                                    | Prend en charge un préprocesseur                                    | Tokenizers compatibles                                   | Prend en charge un postprocessor |
| ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- | -------------------------------------------------------- | --------------------------------- |
| `=`                                                                                         | oui                                                                 | tous                                                     | oui                               |
| `IN`                                                                                        | oui                                                                 | tous                                                     | oui                               |
| [hasToken](/fr/sql-reference/functions/string-search-functions.md/#hasToken)                   | oui                                                                 | tous (conçu pour `splitByNonAlpha`)                      | oui                               |
| [hasAnyTokens(col, str)](/fr/sql-reference/functions/string-search-functions.md/#hasAnyTokens) | oui                                                                 | tous                                                     | oui                               |
| [hasAllTokens(col, str)](/fr/sql-reference/functions/string-search-functions.md/#hasAllTokens) | oui                                                                 | tous                                                     | oui                               |
| [hasAnyTokens(col, arr)](/fr/sql-reference/functions/string-search-functions.md/#hasAnyTokens) | non (les éléments du tableau sont utilisés tels quels comme tokens) | tous                                                     | oui                               |
| [hasAllTokens(col, arr)](/fr/sql-reference/functions/string-search-functions.md/#hasAllTokens) | non (les éléments du tableau sont utilisés tels quels comme tokens) | tous                                                     | oui                               |
| [hasPhrase](/fr/sql-reference/functions/string-search-functions.md/#hasPhrase)                 | oui                                                                 | `splitByNonAlpha`, `splitByString`, `ngrams`, `asciiCJK` | oui                               |
| [startsWith](/fr/sql-reference/functions/string-functions.md/#startsWith)                      | oui                                                                 | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | oui                               |
| [endsWith](/fr/sql-reference/functions/string-functions.md/#endsWith)                          | oui                                                                 | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | oui                               |
| [like](/fr/sql-reference/functions/string-search-functions.md/#like)                           | oui¹                                                                | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`¹  | oui¹                              |
| [match](/fr/sql-reference/functions/string-search-functions.md/#match)                         | oui¹                                                                | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`¹  | oui¹                              |
| [ilike](/fr/sql-reference/functions/string-search-functions.md/#like)                          | oui² (`lower`/`upper` uniquement)                                   | `splitByNonAlpha`, `array`²                              | non²                              |
| [mapContainsKey](/fr/sql-reference/functions/tuple-map-functions#mapContainsKey)               | oui                                                                 | tous                                                     | oui                               |
| [mapContainsValue](/fr/sql-reference/functions/tuple-map-functions#mapContainsValue)           | oui                                                                 | tous                                                     | oui                               |
| [mapContainsKeyLike](/fr/sql-reference/functions/tuple-map-functions#mapContainsKeyLike)       | oui                                                                 | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | oui                               |
| [mapContainsValueLike](/fr/sql-reference/functions/tuple-map-functions#mapContainsValueLike)   | oui                                                                 | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | oui                               |
| [has](/fr/sql-reference/functions/array-functions.md/#has)                                     | oui                                                                 | `array`                                                  | oui                               |
| [hasAny](/fr/sql-reference/functions/array-functions.md/#hasAny)                               | oui                                                                 | `array`                                                  | oui                               |
| [hasAll](/fr/sql-reference/functions/array-functions.md/#hasAll)                               | oui                                                                 | `array`                                                  | oui                               |

¹ `LIKE` et `match` utilisent la lecture directe comme indice pour les tokenizers listés ; sinon, ils se rabattent sur un balayage exhaustif.
`LIKE` prend également en charge une *lecture directe (sans indice)* (activée via `use_text_index_like_evaluation_by_dictionary_scan`) pour les tokenizers `splitByNonAlpha` et `array`, sans préprocesseur ni postprocessor.

² `ILIKE` est uniquement pris en charge via la lecture directe (sans indice) (`use_text_index_like_evaluation_by_dictionary_scan = 1`, tokenizer `splitByNonAlpha` ou `array`).
Il n&#39;existe pas de mécanisme de repli utilisant l&#39;index comme indice : si le paramètre est désactivé ou si le tokenizer ne fait pas partie de l&#39;ensemble pris en charge, l&#39;index n&#39;est pas utilisé pour `ILIKE`.
Le préprocesseur, s&#39;il est présent, doit être `lower` ou `upper` ; les postprocessors ne sont pas pris en charge.

**Expérimental : argument Positions (facultatif)**.

Le paramètre expérimental `positions` (par défaut : `0`) détermine si l’index stocke les positions des tokens.
Lorsqu’il est défini sur `1`, l’index stocke également des données de position (dans un fichier `.pos`), ce qui permet la correspondance exacte d’expressions via des lectures directes pour la fonction [`hasPhrase`](#functions-example-hasphrase).
Le stockage des positions augmente la taille de l’index sur disque ainsi que le coût d’écriture ; il s’agit donc d’une option à activer explicitement.
Le format sur disque n’est pas encore stable ; ce paramètre est donc expérimental et pourra changer dans une future release.
La création d’un index avec `positions = 1` exige donc que le MergeTree setting [`allow_experimental_text_index_positions`](/fr/operations/settings/merge-tree-settings#allow_experimental_text_index_positions) soit activé.
Définissez `positions = 0` (la valeur par défaut) pour conserver un stockage reposant uniquement sur les posting lists ; les index de texte créés sans cet argument restent sans positions.

:::warning
Cet argument est expérimental et ne doit être utilisé que pour les tests.
Définissez le MergeTree setting [`allow_experimental_text_index_positions`](/fr/operations/settings/merge-tree-settings#allow_experimental_text_index_positions) pour activer le stockage des positions.
:::

<details markdown="1">
  <summary>Paramètres avancés optionnels</summary>

  Les valeurs par défaut des paramètres avancés suivants conviennent dans la quasi-totalité des situations.
  Nous ne recommandons pas de les modifier.

  Le paramètre optionnel `dictionary_block_size` (par défaut : 512) spécifie la taille des blocks du dictionnaire en rows.

  Le paramètre optionnel `dictionary_block_frontcoding_compression` (par défaut : 1) spécifie si les blocks du dictionnaire utilisent le front coding comme compression.

  Le paramètre optionnel `posting_list_block_size` (par défaut : 1048576) spécifie la taille des blocks de posting list en rows.

  Le paramètre optionnel `posting_list_codec` (par défaut : `none`) spécifie le codec de la posting list :

  * `none` - les posting lists sont stockées sans compression supplémentaire.
  * `bitpacking` - applique un [codage différentiel (delta)](https://en.wikipedia.org/wiki/Delta_encoding), suivi d’un [bit-packing](https://dev.to/madhav_baby_giraffe/bit-packing-the-secret-to-optimizing-data-storage-and-transmission-m70) (chacun au sein de blocks de taille fixe). Ralentit les requêtes SELECT, non recommandé pour le moment.

  Les paramètres avancés ci-dessus peuvent aussi être définis au niveau de la table via les MergeTree settings correspondants : [`text_index_dictionary_block_size`](/fr/operations/settings/merge-tree-settings#text_index_dictionary_block_size), [`text_index_dictionary_block_frontcoding_compression`](/fr/operations/settings/merge-tree-settings#text_index_dictionary_block_frontcoding_compression), [`text_index_posting_list_block_size`](/fr/operations/settings/merge-tree-settings#text_index_posting_list_block_size) et [`text_index_posting_list_codec`](/fr/operations/settings/merge-tree-settings#text_index_posting_list_codec).
  Ils s’appliquent à chaque index de texte de la table qui ne spécifie pas explicitement le paramètre.

  Le principal cas d’usage des settings au niveau de la table consiste à modifier les paramètres d’index d’une table existante sans supprimer puis recréer l’index de texte sur toutes les table parts.
  La modification d’un setting au niveau de la table applique les nouveaux paramètres uniquement aux index de texte construits pour les nouvelles parts ; les parts existantes conservent leur layout actuel.

  Un argument indiqué dans la définition de l’index a préséance sur le setting de la table, par exemple :

  ```sql
  CREATE TABLE table(
      s String,
      -- Cet index utilise 'bitpacking', remplaçant la valeur par défaut au niveau de la table ci-dessous :
      INDEX idx_a s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking'),
      -- Cet index hérite de 'none' du setting de la table :
      INDEX idx_b lower(s) TYPE text(tokenizer = 'splitByNonAlpha'))
  ENGINE = MergeTree()
  ORDER BY tuple()
  SETTINGS text_index_posting_list_codec = 'none';
  ```
</details>

*Granularité de l’index.*
Les index de texte sont implémentés dans ClickHouse comme un type de [skip indexes](/fr/engines/table-engines/mergetree-family/mergetree.md/#skip-index-types).
Cependant, contrairement aux autres skip indexes, les index de texte utilisent une granularité infinie (100 millions).
Cela peut être observé dans la définition de table d’un index de texte.

Exemple :

```sql title="Query"
CREATE TABLE table(
    k UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = ngrams(2)))
ENGINE = MergeTree()
ORDER BY k;

SHOW CREATE TABLE table;
```

```result title="Response"
┌─statement──────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.table                                            ↴│
│↳(                                                                     ↴│
│↳    `k` UInt64,                                                       ↴│
│↳    `s` String,                                                       ↴│
│↳    INDEX idx s TYPE text(tokenizer = ngrams(2)) GRANULARITY 100000000↴│ <-- here
│↳)                                                                     ↴│
│↳ENGINE = MergeTree                                                    ↴│
│↳ORDER BY k                                                            ↴│
│↳SETTINGS index_granularity = 8192                                      │
└────────────────────────────────────────────────────────────────────────┘
```

La granularité d’index très élevée garantit que l’index de texte est créé sur l’ensemble de la part.
Une granularité d’index explicitement spécifiée est ignorée.

<div id="using-a-text-index">
  ## Utiliser un index de texte
</div>

L’utilisation d’un index de texte dans les requêtes SELECT est simple, car les fonctions courantes de recherche de chaînes s’appuient automatiquement sur l’index.
Si aucun index n’existe sur une colonne ou une part de la table, les fonctions de recherche de chaînes retomberont sur de lents balayages exhaustifs.

:::note
Nous recommandons d’utiliser les fonctions `hasAnyTokens` et `hasAllTokens` pour interroger l’index de texte ; voir [ci-dessous](#functions-example-hasanytokens-hasalltokens).
Ces fonctions sont compatibles avec tous les tokenizers disponibles ainsi qu’avec toutes les expressions de préprocesseur et de postprocesseur possibles.
Comme les autres fonctions prises en charge sont historiquement antérieures à l’index de texte, elles ont dû conserver leur comportement legacy dans de nombreux cas (par exemple, sans prise en charge du préprocesseur ni du postprocesseur).
:::

<div id="functions-support">
  ### Fonctions prises en charge
</div>

L’index de texte peut être utilisé lorsque des fonctions textuelles sont employées dans la clause `WHERE` ou les clauses `PREWHERE` :

```sql
SELECT [...]
FROM [...]
WHERE string_search_function(column_with_text_index)
```

<div id="functions-example-equals">
  #### `=`
</div>

`=` ([equals](/fr/sql-reference/functions/comparison-functions.md/#equals)) correspond exactement à l’intégralité du terme de recherche indiqué.

Exemple :

```sql
SELECT * from table WHERE str = 'Hello';
```

<div id="functions-example-in">
  #### `IN`
</div>

`IN` ([in](/fr/sql-reference/functions/in-functions)) est similaire à `equals`, mais correspond à l’ensemble des termes de recherche.

Exemple :

```sql
SELECT * from table WHERE str IN ('Hello', 'World');
```

:::note
`NOT IN` (`notIn`) n’est pas pris en charge par l’index de texte.
:::

<div id="functions-example-like-match">
  #### `LIKE` et `match`
</div>

:::note
Ces fonctions utilisent actuellement l&#39;index de texte pour le filtrage uniquement si le tokenizer de l&#39;index est `splitByNonAlpha`, `ngrams` ou `sparseGrams`.
:::

:::note
`NOT LIKE` (`notLike`) n&#39;est pas pris en charge par l&#39;index de texte.
:::

Pour utiliser `LIKE` ([like](/fr/sql-reference/functions/string-search-functions.md/#like)) et la fonction [match](/fr/sql-reference/functions/string-search-functions.md/#match) avec des index de texte, ClickHouse doit pouvoir extraire des tokens complets du terme de recherche.
Pour un index utilisant le tokenizer `ngrams`, c&#39;est le cas si la longueur des chaînes recherchées entre les caractères génériques est égale ou supérieure à la longueur du ngram.

Exemple d&#39;index de texte avec le tokenizer `splitByNonAlpha` :

```sql
SELECT count() FROM table WHERE comment LIKE 'support%';
```

`support` dans l’exemple pourrait correspondre à `support`, `supports`, `supporting`, etc.
Ce type de requête est une requête de sous-chaîne et ne peut pas être accéléré par un index de texte.

Pour tirer parti d’un index de texte pour les requêtes LIKE, le motif LIKE doit être réécrit de la manière suivante :

```sql
SELECT count() FROM table WHERE comment LIKE ' support %'; -- or `% support %`
```

Les espaces de part et d’autre de `support` garantissent que le terme peut être extrait sous forme de token.

Heureusement, il existe un cas particulier dans lequel ClickHouse peut exploiter l’index inversé pour accélérer considérablement les requêtes LIKE.

Consultez la [section sur l’optimisation des performances de LIKE/ILIKE](#like-ilike-queries-perf) pour plus de détails.

<div id="functions-example-multisearchany-multimatchany">
  #### `multiSearchAny` et `multiMatchAny`
</div>

[multiSearchAny](/fr/sql-reference/functions/string-search-functions.md/#multiSearchAny) et sa variante UTF-8 [multiSearchAnyUTF8](/fr/sql-reference/functions/string-search-functions.md/#multiSearchAnyUTF8) vérifient si l’une de plusieurs sous-chaînes littérales est présente dans la chaîne source, et [multiMatchAny](/fr/sql-reference/functions/string-search-functions.md/#multiMatchAny) vérifie si l’une de plusieurs expressions régulières correspond.
Ces fonctions utilisent l’index de texte dans les mêmes conditions que `LIKE` et `match` (voir ci-dessus) : ClickHouse doit pouvoir extraire des tokens complets de chaque motif recherché, et la liste des motifs recherchés doit être constante.
Un granule est lu si l’un des motifs recherchés peut s’y trouver.

Pour `multiMatchAny`, si un seul motif ne peut pas être ramené à une contrainte de token (par exemple `.*`, qui correspond à n’importe quel document), l’index de texte ne peut pas être utilisé et la requête revient à un parcours complet.

Comme avec `LIKE` et `match`, la recherche par sous-chaîne et par expression régulière fonctionne le mieux avec les tokenizers `ngrams` et `sparseGrams`.
Ces tokenizers indexent des n-grams de caractères qui se chevauchent, de sorte qu’un motif recherché est décomposé en n-grams présents dans l’index partout où il apparaît comme sous-chaîne, qu’il commence ou se termine au milieu d’un mot ou non.
Un motif recherché peut donc être utilisé tel quel, à condition qu’il soit au moins aussi long que la taille du n-gram.

Exemple d’index de texte avec le tokenizer `ngrams` :

```sql
SELECT count() FROM table WHERE multiSearchAny(comment, ['clickhouse', 'support']);
```

Le tokenizer `splitByNonAlpha`, en revanche, n&#39;indexe que des tokens complets (des mots entiers).
Comme une chaîne recherchée peut commencer ou se terminer au milieu d&#39;un mot, ClickHouse supprime les tokens de début et de fin de chaque chaîne recherchée, de sorte que l&#39;index ne puisse exclure des granules qu&#39;à partir de tokens complets.
Pour que la recherche par sous-chaîne et par expression régulière utilise l&#39;index avec `splitByNonAlpha`, entourez chaque chaîne recherchée de caractères séparateurs (par exemple des espaces) afin qu&#39;elle forme un ou plusieurs tokens complets.

Exemple d&#39;index de texte avec le tokenizer `splitByNonAlpha` :

```sql
SELECT count() FROM table WHERE multiSearchAny(comment, [' clickhouse ', ' support ']);
```

<div id="functions-example-startswith-endswith">
  #### `startsWith` and `endsWith`
</div>

Comme pour `LIKE`, les fonctions [startsWith](/fr/sql-reference/functions/string-functions.md/#startsWith) et [endsWith](/fr/sql-reference/functions/string-functions.md/#endsWith) ne peuvent utiliser un index de texte que si des tokens complets peuvent être extraits du terme de recherche.
Pour l&#39;index avec le tokenizer `ngrams`, c&#39;est le cas si la longueur des chaînes recherchées entre les wildcards est égale ou supérieure à la longueur du ngram.
Lorsqu&#39;un index de texte utilise un postprocesseur, ces fonctions peuvent toujours utiliser l&#39;index en mode Hint si les tokens d&#39;indice extraits restent non vides après normalisation. Si la normalisation supprime tous les tokens d&#39;indice, l&#39;index n&#39;est pas utilisé pour ce prédicat.

Exemple d’index de texte avec le tokenizer `splitByNonAlpha` :

```sql
SELECT count() FROM table WHERE startsWith(comment, 'clickhouse support');
```

Dans l’exemple, seul `clickhouse` est considéré comme un token.
`support` n’est pas un token, car il peut correspondre à `support`, `supports`, `supporting`, etc.

Pour trouver toutes les lignes qui commencent par `clickhouse supports`, veuillez terminer le motif de recherche par un espace final :

```sql
startsWith(comment, 'clickhouse supports ')`
```

De même, `endsWith` doit être utilisé avec un espace initial :

```sql
SELECT count() FROM table WHERE endsWith(comment, ' olap engine');
```

<div id="functions-example-hastoken">
  #### `hasToken`
</div>

:::note
`hasToken` présente certaines limites lorsqu’elle est utilisée pour des recherches dans des index de texte avec des tokenizers autres que `splitByNonAlpha` et/ou des expressions de préprocesseur/postprocesseur.
Nous vous recommandons d’utiliser `hasAnyTokens` et `hasAllTokens` à la place.

Les variantes insensibles à la casse `hasTokenCaseInsensitive` et `hasTokenCaseInsensitiveOrNull` ne prennent pas en charge les index de texte : elles effectuent toujours un parcours complet des lignes, même sur des colonnes dotées d’un index de texte. Pour une correspondance insensible à la casse, utilisez un préprocesseur ou un postprocesseur `lower(...)` et combinez-le avec `hasToken` / `hasAllTokens` / `hasAnyTokens`.
:::

La fonction [hasToken](/fr/sql-reference/functions/string-search-functions.md/#hasToken) recherche un seul token donné.

Contrairement aux fonctions mentionnées précédemment, cette fonction ne tokenise pas le terme de recherche (elle suppose que l’entrée est un seul token).

Exemple :

```sql
SELECT count() FROM table WHERE hasToken(comment, 'clickhouse');
```

<div id="functions-example-hasanytokens-hasalltokens">
  #### `hasAnyTokens` and `hasAllTokens`
</div>

Les fonctions [hasAnyTokens](/fr/sql-reference/functions/string-search-functions.md/#hasAnyTokens) et [hasAllTokens](/fr/sql-reference/functions/string-search-functions.md/#hasAllTokens) effectuent la correspondance avec un ou tous les tokens fournis.

Ces deux fonctions acceptent les tokens de recherche soit sous la forme d’une chaîne, qui sera tokenisée à l’aide du même tokenizer que celui utilisé pour la colonne indexée, soit sous la forme d’un tableau de tokens déjà traités, auxquels aucune tokenisation ne sera appliquée avant la recherche.
Consultez la documentation de la fonction pour plus d’informations.

Exemple :

```sql
-- Search tokens passed as string argument
SELECT count() FROM table WHERE hasAnyTokens(comment, 'clickhouse olap');
SELECT count() FROM table WHERE hasAllTokens(comment, 'clickhouse olap');

-- Search tokens passed as Array(String)
SELECT count() FROM table WHERE hasAnyTokens(comment, ['clickhouse', 'olap']);
SELECT count() FROM table WHERE hasAllTokens(comment, ['clickhouse', 'olap']);
```

<div id="functions-example-hasphrase">
  #### `hasPhrase`
</div>

La fonction [hasPhrase](/fr/sql-reference/functions/string-search-functions.md/#hasPhrase) recherche une expression : tous les tokens doivent apparaître de manière consécutive et dans le même ordre que dans la chaîne de recherche.

Contrairement à `hasAllTokens`, qui exige seulement la présence de tous les tokens quelque part, `hasPhrase` exige qu’ils apparaissent sous la forme d’une séquence continue.
L’expression de recherche est découpée en tokens à l’aide du même tokenizer configuré pour la colonne d’index.
Lorsque l’index de texte utilise un postprocesseur, l’expression de recherche est également normalisée avant la recherche dans l’index.
Notez que cette fonction nécessite l’un des tokenizers `splitByNonAlpha`, `splitByString`, `ngrams` ou `asciiCJK`.

Exemple :

```sql
-- Matches: 'clickhouse' and 'olap' must appear consecutively in that order
SELECT count() FROM table WHERE hasPhrase(comment, 'clickhouse olap');

-- Does NOT match a row containing 'olap clickhouse' (wrong order)
-- Does NOT match a row containing 'clickhouse fast olap' (non-consecutive)
```

<div id="functions-example-has">
  #### `has`
</div>

La fonction de tableau [has](/fr/sql-reference/functions/array-functions#has) vérifie la présence d’un seul token dans le tableau de chaînes de caractères.

Exemple :

```sql
SELECT count() FROM table WHERE has(array, 'clickhouse');
```

<div id="functions-example-hasany-hasall">
  #### `hasAny` et `hasAll`
</div>

Les fonctions sur les tableaux [hasAny](/fr/sql-reference/functions/array-functions#hasAny) et [hasAll](/fr/sql-reference/functions/array-functions#hasAll) vérifient si la colonne de type Array indexée contient au moins une ou la totalité d’un ensemble constant de chaînes recherchées.

Exemple :

```sql
SELECT count() FROM table WHERE hasAny(tags, ['clickhouse', 'olap']);
SELECT count() FROM table WHERE hasAll(tags, ['clickhouse', 'olap']);
```

<div id="functions-example-mapcontains">
  #### `mapContains`
</div>

La fonction [mapContains](/fr/sql-reference/functions/tuple-map-functions#mapContainsKey) (alias de `mapContainsKey`) recherche des correspondances parmi les tokens extraits de la chaîne recherchée dans les clés d’une map.
Le comportement est similaire à celui de la fonction `equals` sur une colonne `String`.
L’index de texte n’est utilisé que s’il a été créé sur une expression `mapKeys(map)`.

Exemple :

```sql
SELECT count() FROM table WHERE mapContainsKey(map, 'clickhouse');
-- OR
SELECT count() FROM table WHERE mapContains(map, 'clickhouse');
```

<div id="functions-example-mapcontainsvalue">
  #### `mapContainsValue`
</div>

La fonction [mapContainsValue](/fr/sql-reference/functions/tuple-map-functions#mapContainsValue) fait correspondre les tokens extraits de la chaîne recherchée dans les valeurs d’une map.
Son comportement est similaire à celui de la fonction `equals` appliquée à une colonne `String`.
L’index de texte n’est utilisé que s’il a été créé sur une expression `mapValues(map)`.

Exemple :

```sql
SELECT count() FROM table WHERE mapContainsValue(map, 'clickhouse');
```

<div id="functions-example-mapcontainslike">
  #### `mapContainsKeyLike` et `mapContainsValueLike`
</div>

Les fonctions [mapContainsKeyLike](/fr/sql-reference/functions/tuple-map-functions#mapContainsKeyLike) et [mapContainsValueLike](/fr/sql-reference/functions/tuple-map-functions#mapContainsValueLike) font correspondre un motif à toutes les clés ou valeurs (respectivement) d’une map.

Exemple :

```sql
SELECT count() FROM table WHERE mapContainsKeyLike(map, '% clickhouse %');
SELECT count() FROM table WHERE mapContainsValueLike(map, '% clickhouse %');
```

<div id="functions-example-access-operator">
  #### `operator[]`
</div>

L’opérateur d’accès [operator[]](/fr/sql-reference/operators#access-operators) peut être utilisé avec l’index de texte afin de filtrer les clés et les valeurs. L’index de texte n’est utilisé que s’il est créé sur les expressions `mapKeys(map)` ou `mapValues(map)`, ou sur les deux.

Exemple :

```sql
SELECT count() FROM table WHERE map['engine'] = 'clickhouse';
```

Consultez les exemples ci-dessous pour utiliser des colonnes de type `Array(T)` et `Map(K, V)` avec l’index de texte.

<div id="text-index-example-array">
  ### Indexation des colonnes Array(String)
</div>

Imaginez une plateforme de blogs, où les auteurs classent leurs articles à l’aide de mots-clés.
Nous souhaitons que les utilisateurs découvrent des contenus associés en recherchant des thèmes ou en cliquant dessus.

Considérez la définition de table suivante :

```sql
CREATE TABLE posts
(
    post_id UInt64,
    title String,
    content String,
    keywords Array(String)
)
ENGINE = MergeTree
ORDER BY (post_id);
```

Sans index de texte, trouver les posts contenant un mot-clé spécifique (par ex. `clickhouse`) nécessite de parcourir toutes les entrées :

```sql
SELECT count() FROM posts WHERE has(keywords, 'clickhouse'); -- slow full-table scan - checks every keyword in every post
```

À mesure que la plateforme grandit, cela devient de plus en plus lent, car la requête doit examiner le tableau `keywords` de chaque ligne.
Pour remédier à ce problème de performances, nous définissons un index de texte pour la colonne `keywords` :

```sql
ALTER TABLE posts ADD INDEX keywords_idx(keywords) TYPE text(tokenizer = splitByNonAlpha);
ALTER TABLE posts MATERIALIZE INDEX keywords_idx; -- Don't forget to rebuild the index for existing data
```

<div id="text-index-example-map">
  ### Indexation des colonnes Map
</div>

Dans de nombreux cas d’usage en observabilité, les messages de log sont décomposés en &quot;composants&quot; et stockés dans des types de données appropriés, par ex. date et heure pour le timestamp, enum pour le niveau de log, etc.
Les champs de métriques sont idéalement stockés sous forme de paires clé-valeur.
Les équipes d’exploitation doivent pouvoir rechercher efficacement dans les logs à des fins de débogage, d’incidents de sécurité et de supervision.

Considérez cette table de logs :

```sql
CREATE TABLE logs
(
    id UInt64,
    timestamp DateTime,
    message String,
    attributes Map(String, String)
)
ENGINE = MergeTree
ORDER BY (timestamp);
```

Sans index de texte, effectuer des recherches dans des données [Map](/fr/sql-reference/data-types/map.md) exige un parcours complet de la table :

```sql
-- Finds all logs with rate limiting data:
SELECT * FROM logs WHERE has(mapKeys(attributes), 'rate_limit'); -- slow full-table scan

-- Finds all logs from a specific IP:
SELECT * FROM logs WHERE has(mapValues(attributes), '192.168.1.1'); -- slow full-table scan
```

À mesure que le volume de logs augmente, ces requêtes deviennent lentes.

La solution consiste à créer un index de texte pour les clés et les valeurs de [Map](/fr/sql-reference/data-types/map.md).
Utilisez [mapKeys](/fr/sql-reference/functions/tuple-map-functions.md/#mapKeys) pour créer un index de texte lorsque vous devez retrouver des logs à partir de noms de champ ou de types d’attribut :

```sql
ALTER TABLE logs ADD INDEX attributes_keys_idx mapKeys(attributes) TYPE text(tokenizer = array);
ALTER TABLE posts MATERIALIZE INDEX attributes_keys_idx;
```

Utilisez [mapValues](/fr/sql-reference/functions/tuple-map-functions.md/#mapValues) pour créer un index de texte lorsque vous devez rechercher dans le contenu même des attributs :

```sql
ALTER TABLE logs ADD INDEX attributes_vals_idx mapValues(attributes) TYPE text(tokenizer = array);
ALTER TABLE posts MATERIALIZE INDEX attributes_vals_idx;
```

Exemples de requêtes :

```sql
-- Find all rate-limited requests:
SELECT * FROM logs WHERE mapContainsKey(attributes, 'rate_limit'); -- fast

-- Finds all logs from a specific IP:
SELECT * FROM logs WHERE has(mapValues(attributes), '192.168.1.1'); -- fast

-- Finds all logs where any attribute includes an error:
SELECT * FROM logs WHERE mapContainsValueLike(attributes, '% error %'); -- fast
```

<div id="text-index-example-json">
  ### Indexation des colonnes JSON
</div>

Les index de texte peuvent être utilisés avec des colonnes `JSON` de trois façons :

1. **Index sur des sous-colonnes spécifiques** — créez un index de texte sur un chemin JSON connu, comme pour une colonne classique. Cela indexe les *valeurs* de ce chemin.
2. **Index basés sur les chemins avec [JSONAllPaths](/fr/sql-reference/functions/json-functions.md/#JSONAllPaths)** — indexent *tous les chemins* présents dans chaque granule afin d’ignorer celles qui ne peuvent pas contenir le chemin recherché. Comme pour les colonnes `Map`.
3. **Index basés sur les valeurs avec [JSONAllValues](/fr/sql-reference/functions/json-functions.md#JSONAllValues)** — indexent *toutes les valeurs* de tous les chemins JSON afin d’accélérer la recherche en texte intégral sur n’importe quelle sous-colonne JSON avec un seul index.

<div id="json-indexes-on-subcolumns">
  #### Index sur des sous-colonnes spécifiques
</div>

Vous pouvez créer un skip index sur n’importe quelle sous-colonne JSON en utilisant la même syntaxe que pour les colonnes ordinaires.

Il existe deux façons de référencer une sous-colonne JSON dans l’expression d’un index :

* **Chemin typé** déclaré dans l’indication de type JSON — accès direct par son nom : `json.a`.
* **Chemin Dynamic** avec conversion de type explicite — utilisez la syntaxe de transtypage `::` : `json.b::String`.

Exemple de définition d’index :

```sql title="Query"
CREATE TABLE sensor_data
(
    data JSON(sensor_id String),
    INDEX idx_sensor data.sensor_id TYPE text(tokenizer = splitByNonAlpha),
    INDEX idx_location data.location::String TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO sensor_data SELECT toJSONString(map('sensor_id', 'id_' || number , 'location', 'room_' || toString(number))) FROM numbers(4);
INSERT INTO sensor_data SELECT toJSONString(map('sensor_id', 'id_' || number, 'location', 'room_' || toString(number))) FROM numbers(4, 4);
```

Exemple de requête :

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM sensor_data WHERE data.sensor_id = 'id_5';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx_sensor
        Description: text
        Condition: (mode: All; tokens: ["5", "id"])
        Parts: 1/2
        Granules: 1/8
```

Exemple de requête :

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM sensor_data WHERE data.location::String = 'room_5';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx_location
        Description: text
        Condition: (mode: All; tokens: ["5", "room"])
        Parts: 1/2
        Granules: 1/8
```

<div id="json-indexes-jsonallpaths">
  #### Index basés sur les chemins avec JSONAllPaths
</div>

Comme pour les colonnes `Map`, des index de texte peuvent être créés sur des colonnes [JSON](/fr/sql-reference/data-types/newjson.md) à l’aide de [`JSONAllPaths`](/fr/sql-reference/functions/json-functions.md/#JSONAllPaths).
L’index stocke l’ensemble des chemins JSON présents dans chaque granule et s’en sert pour ignorer les granules dans lesquels le chemin recherché est absent.

Exemple de définition d’index :

```sql title="Query"
CREATE TABLE events
(
    data JSON,
    INDEX idx JSONAllPaths(data) TYPE text(tokenizer = array)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO events VALUES ('{"user": {"name": "Alice"}, "action": "login"}');
INSERT INTO events VALUES ('{"metric": {"cpu": 0.95}, "host": "srv1"}');
```

Vous pouvez utiliser `EXPLAIN indexes = 1` pour vérifier que le skip index est bien utilisé.
Lorsqu’un chemin n’existe que dans une seule partie, l’index permet d’ignorer l’autre partie.

Exemple :

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.user.name = 'Alice';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: text
        Condition: (mode: All; tokens: ["user.name"])
        Parts: 1/2
        Granules: 1/2
```

Si un chemin n’existe dans aucune part, toutes les parts et les granules sont ignorées.

Exemple :

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.nonexistent = 1;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: text
        Condition: (mode: All; tokens: ["nonexistent"])
        Parts: 0/2
        Granules: 0/2
```

`IS NOT NULL` utilise lui aussi l’index — il ignore les granules où le chemin est absent (puisque la valeur serait `NULL`) :

Exemple :

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.user.name IS NOT NULL;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: text
        Condition: (mode: All; tokens: ["user.name"])
        Parts: 1/2
        Granules: 1/2
```

<div id="json-indexes-jsonallvalues">
  #### Index basés sur les valeurs avec JSONAllValues
</div>

Les index de texte peuvent être utilisés pour accélérer les recherches dans les colonnes [JSON](/fr/sql-reference/data-types/newjson.md) via la fonction [`JSONAllValues`](/fr/sql-reference/functions/json-functions.md#JSONAllValues).

`JSONAllValues` renvoie toutes les valeurs d&#39;une colonne JSON sous forme de `Array(String)`.
Les valeurs de types de données non textuels (par ex. les entiers et les tableaux) sont converties en leur représentation textuelle.
Un index de texte construit avec `JSONAllValues` indexe ces représentations textuelles sur l&#39;ensemble des chemins JSON de chaque ligne.
Cet index peut ensuite accélérer les requêtes qui filtrent sur des sous-colonnes JSON spécifiques.
Lorsqu&#39;une requête filtre sur une sous-colonne précise (par ex. `data.user_name = 'alice'`), l&#39;index de texte peut rapidement ignorer les lignes (et les granules) qui ne contiennent pas les tokens recherchés dans leurs valeurs JSON.

:::note
L&#39;index peut produire des faux positifs lorsque différents chemins JSON contiennent les mêmes tokens.
Par exemple, si la ligne 1 contient `{"a": "hello", "b": "world"}` et qu&#39;une requête recherche `data.a = 'world'`, l&#39;index de texte ne peut pas distinguer que `world` appartient au chemin `b`, et non à `a`.
Dans ce cas, l&#39;index n&#39;ignorera pas la ligne, et le filtre sur les données réelles de la colonne effectuera l&#39;évaluation finale.
Il s&#39;agit du même comportement que dans d&#39;autres cas d&#39;utilisation des index de texte, où l&#39;index agit comme un préfiltre rapide.
:::

<div id="json-all-values-creating-the-index">
  ##### Création de l’index
</div>

Exemple de définition d’un index :

```sql
CREATE TABLE events
(
    id UInt64,
    data JSON,
    INDEX json_idx JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;
```

<div id="json-all-values-supported-query-patterns">
  ##### Modèles de requête pris en charge
</div>

Une fois l’index créé, il peut accélérer les requêtes sur les sous-colonnes JSON en utilisant les mêmes fonctions que pour les colonnes `String`, ainsi que la fonction `equals` pour toutes les colonnes.

Accès aux sous-colonnes :

```sql
SELECT * FROM events WHERE data.user_name = 'alice';
SELECT * FROM events WHERE data.message LIKE '% error %';
SELECT * FROM events WHERE startsWith(data.status, 'fail');
SELECT * FROM events WHERE hasToken(data.title, 'clickhouse');
```

Accès aux sous-colonnes avec `CAST` explicite :

```sql
SELECT * FROM events WHERE hasAllTokens(data.message::String, 'connection timeout');
SELECT * FROM events WHERE data.status_code::UInt64 = 404;
SELECT * FROM events WHERE has(data.tags::Array(String), 'bug')
```

opérateur `IN` :

```sql
SELECT * FROM events WHERE data.level IN ('error', 'critical');
```

<div id="text-index-phrase-search">
  ### Recherche d’expression exacte
</div>

Une recherche classique dans un index de texte, par exemple

```sql
SELECT *
FROM tab
WHERE hasAllTokens(col, 'weather in Tokyo')
```

correspond à toutes les lignes qui contiennent les tokens indiqués, dans n’importe quel ordre.
Dans l’exemple, la ligne `While she stayed in Tokyo, the weather was great.` correspond au filtre.

En revanche, une recherche de phrase consiste à faire correspondre les tokens dans l’ordre indiqué.
Par exemple,

```sql
SELECT *
FROM tab
WHERE hasPhrase(col, 'weather in Tokyo')
```

correspond à toute ligne contenant la séquence de tokens `weather in Tokyo`, comme `How is the weather in Tokyo?` ?

L’index de texte accélère la recherche de phrases en croisant les listes de postings de tous les tokens de la phrase afin d’identifier les granules candidates.
Dans ces granules, ClickHouse vérifie ensuite l’adjacence exacte des tokens.
Ce processus est relativement coûteux et plus lent que les requêtes de recherche textuelle classiques.
Pour accélérer les requêtes de recherche de phrases, veuillez activer le stockage des positions dans l’index de texte (voir `Optional parameters` ci-dessus).

`hasPhrase` peut être utilisé avec les tokenizers `splitByNonAlpha`, `splitByString`, `ngrams` et `asciiCJK`.
La chaîne de la phrase fournie est tokenisée à l’aide du tokenizer de l’index.
Les caractères séparateurs de la phrase sont ignorés : `hasPhrase(text, 'quick+brown')` est équivalent à `hasPhrase(text, 'quick brown')`, en supposant que `splitByNonAlpha` soit utilisé comme tokenizer.

<div id="text-index-phrase-search-example">
  #### Exemple
</div>

```sql
CREATE TABLE tab (
    id UInt32,
    text String,
    INDEX idx text TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES
    (1, 'weather in New York'),
    (2, 'New weather in York'),
    (3, 'weather in New Orleans');
```

```sql title="Query"
SELECT id, text FROM tab WHERE hasPhrase(text, 'weather in New York');
```

```result title="Response"
   ┌─id─┬─text────────────────┐
1. │  1 │ weather in New York │
   └────┴─────────────────────┘
```

La ligne 2 (`'New weather in York'`) ne correspond pas, car les tokens ne sont pas dans le bon ordre.
La ligne 3 (`'weather in New Orleans'`) ne correspond pas, car elle ne contient pas le token `'York'`.

<div id="performance-tuning">
  ## Optimisation des performances
</div>

<div id="direct-read">
  ### Direct read
</div>

Certains types de requêtes textuelles peuvent être nettement accélérés grâce à une optimisation appelée &quot;direct read&quot;.

Exemple :

```sql
SELECT column_a, column_b, ...
FROM [...]
WHERE string_search_function(column_with_text_index)
```

L’optimisation de lecture directe répond à la requête exclusivement à l’aide de l’index de texte (c.-à-d. via des consultations de l’index de texte), sans accéder à la colonne de texte sous-jacente.
Les consultations de l’index de texte lisent relativement peu de données et sont donc bien plus rapides que les skip indexes habituels dans ClickHouse (qui effectuent une consultation de skip index, suivie du chargement et du filtrage des granules restantes).

La lecture directe est contrôlée par deux paramètres :

* Le paramètre [query&#95;plan&#95;direct&#95;read&#95;from&#95;text&#95;index](../../../operations/settings/settings#query_plan_direct_read_from_text_index) (`true` par défaut), qui indique si la lecture directe est activée de manière générale.
* Le paramètre [use&#95;skip&#95;indexes&#95;on&#95;data&#95;read](../../../operations/settings/settings#use_skip_indexes_on_data_read) était un prérequis pour la lecture directe dans les versions de ClickHouse &lt; 26.4.

**Fonctions prises en charge**

L’optimisation de lecture directe prend en charge les fonctions `hasToken`, `hasAllTokens` et `hasAnyTokens`.
Si l’index de texte est défini avec un tokenizer `array`, la lecture directe est également prise en charge pour les fonctions `equals`, `has`, `hasAny`, `hasAll`, `mapContainsKey` et `mapContainsValue`.
Ces fonctions peuvent aussi être combinées avec les opérateurs `AND`, `OR` et `NOT`.
Les clauses `WHERE` ou `PREWHERE` peuvent également contenir des filtres supplémentaires qui ne sont pas des fonctions de recherche textuelle (pour les colonnes de texte ou d’autres colonnes) - dans ce cas, l’optimisation de lecture directe sera tout de même utilisée, mais sera moins efficace (elle s’applique uniquement aux fonctions de recherche textuelle prises en charge).

Pour vérifier qu’une requête utilise la lecture directe, exécutez-la avec `EXPLAIN PLAN actions = 1`.
Par exemple, une requête avec la lecture directe désactivée

```sql
EXPLAIN PLAN actions = 1
SELECT count()
FROM table
WHERE hasToken(col, 'some_token')
SETTINGS query_plan_direct_read_from_text_index = 0, -- disable direct read
```

renvoie

```text
[...]
Filter ((WHERE + Change column names to column identifiers))
Filter column: hasToken(__table1.col, 'some_token'_String) (removed)
Actions: INPUT : 0 -> col String : 0
         COLUMN Const(String) -> 'some_token'_String String : 1
         FUNCTION hasToken(col :: 0, 'some_token'_String :: 1) -> hasToken(__table1.col, 'some_token'_String) UInt8 : 2
[...]
```

tandis que la même requête est exécutée avec `query_plan_direct_read_from_text_index = 1`

```sql
EXPLAIN PLAN actions = 1
SELECT count()
FROM table
WHERE hasToken(col, 'some_token')
SETTINGS query_plan_direct_read_from_text_index = 1, -- enable direct read
```

renvoie

```text
[...]
Expression (Before GROUP BY)
Positions:
  Filter
  Filter column: __text_index_idx_hasToken_94cc2a813036b453d84b6fb344a63ad3 (removed)
  Actions: INPUT :: 0 -> __text_index_idx_hasToken_94cc2a813036b453d84b6fb344a63ad3 UInt8 : 0
[...]
```

La sortie du second EXPLAIN PLAN contient une colonne virtuelle `__text_index_<index_name>_<function_name>_<id>`.
Si cette colonne est présente, la lecture directe est utilisée.

Si la clause WHERE ne contient que des fonctions de recherche textuelle, la requête peut éviter complètement de lire les données de la colonne et tirer le maximum de bénéfices en termes de performances de la lecture directe.
Cependant, même si la colonne de texte est utilisée ailleurs dans la requête, la lecture directe apportera tout de même un gain de performances.

**La lecture directe comme indication**

La lecture directe comme indication repose sur les mêmes principes que la lecture directe normale, mais ajoute à la place un filtre supplémentaire construit à partir des données de l’index de texte, sans supprimer la colonne de texte sous-jacente.
Elle est utilisée pour les fonctions pour lesquelles une lecture uniquement depuis l’index de texte produirait des faux positifs.

Les fonctions prises en charge sont : `like`, `startsWith`, `endsWith`, `equals`, `has`, `hasPhrase`, `mapContainsKey` et `mapContainsValue`.

Le filtre supplémentaire peut apporter davantage de sélectivité pour restreindre encore le jeu de résultats en combinaison avec d’autres filtres, ce qui contribue à réduire la quantité de données lues depuis les autres colonnes.

La lecture directe comme indication est contrôlée par le paramètre [query&#95;plan&#95;text&#95;index&#95;add&#95;hint](../../../operations/settings/settings#query_plan_text_index_add_hint) (activé par défaut).

Exemple de requête sans indication :

```sql
EXPLAIN actions = 1
SELECT count()
FROM table
WHERE (col LIKE '%some-token%') AND (d >= today())
SETTINGS query_plan_text_index_add_hint = 0
FORMAT TSV
```

renvoie

```text
[...]
Prewhere filter column: and(like(__table1.col, \'%some-token%\'_String), greaterOrEquals(__table1.d, _CAST(20440_Date, \'Date\'_String))) (removed)
[...]
```

alors que la même requête, exécutée avec `query_plan_text_index_add_hint = 1`

```sql
EXPLAIN actions = 1
SELECT count()
FROM table
WHERE col LIKE '%some-token%'
SETTINGS query_plan_text_index_add_hint = 1
```

renvoie

```text
[...]
Prewhere filter column: and(__text_index_idx_col_like_d306f7c9c95238594618ac23eb7a3f74, like(__table1.col, \'%some-token%\'_String), greaterOrEquals(__table1.d, _CAST(20440_Date, \'Date\'_String))) (removed)
[...]
```

Dans le deuxième résultat d’EXPLAIN PLAN, vous pouvez voir qu’une conjonction supplémentaire (`__text_index_...`) a été ajoutée à la condition de filtrage.
Grâce à l’optimisation [PREWHERE](/fr/sql-reference/statements/select/prewhere), la condition de filtrage est décomposée en trois conjonctions distinctes, appliquées dans l’ordre de complexité de calcul croissante.
Pour cette requête, l’ordre d’application est `__text_index_...`, puis `greaterOrEquals(...)`, et enfin `like(...)`.
Cet ordre permet d’ignorer encore plus de granules de données que celles déjà ignorées par l’index de texte et le filtre d’origine, avant de lire les colonnes volumineuses utilisées dans la requête après la clause `WHERE`, ce qui réduit encore la quantité de données à lire.

<div id="like-ilike-queries-perf">
  ### Requêtes LIKE/ILIKE
</div>

Lorsqu’un motif de requête LIKE/ILIKE est `%<caractères-alphanumériques-sans-espaces>%` et que le tokenizer de l’index de texte est `splitByNonAlpha` ou `array`, ClickHouse s’appuie sur l’index inversé pour accélérer considérablement les requêtes LIKE/ILIKE. Pour ce faire, ClickHouse parcourt le dictionnaire de l’index inversé au lieu d’effectuer un parcours complet de la table afin de trouver le motif correspondant.

Lorsque l’optimisation est activée, les requêtes LIKE/ILIKE devraient être nettement plus rapides qu’un parcours complet de la table. Toutefois, lorsque le motif correspond à la plupart des tokens du dictionnaire, les performances peuvent être moins bonnes qu’avec un parcours complet de la table. Heureusement, un mécanisme de secours permet d’éviter cela.

L’optimisation est contrôlée par un paramètre :

* [use&#95;text&#95;index&#95;like&#95;evaluation&#95;by&#95;dictionary&#95;scan](../../../operations/settings/settings#use_text_index_like_evaluation_by_dictionary_scan)

Le mécanisme de secours est contrôlé par deux paramètres :

* [text&#95;index&#95;like&#95;min&#95;pattern&#95;length](../../../operations/settings/settings#text_index_like_min_pattern_length)
* [text&#95;index&#95;like&#95;max&#95;postings&#95;to&#95;read](../../../operations/settings/settings#text_index_like_max_postings_to_read)

Cette optimisation prend uniquement en charge les fonctions `like` et `ilike`.

<div id="caching">
  ### Mise en cache
</div>

Il existe différents caches globaux au niveau du serveur pour conserver en mémoire certaines parties de l’index de texte (voir la section [Détails d’implémentation](#implementation)) :
Actuellement, des caches sont disponibles pour les en-têtes, les tokens et les listes de postings désérialisés de l’index de texte afin de réduire les E/S.
Utilisez les paramètres [use&#95;text&#95;index&#95;header&#95;cache](/fr/operations/settings/settings#use_text_index_header_cache), [use&#95;text&#95;index&#95;tokens&#95;cache](/fr/operations/settings/settings#use_text_index_tokens_cache) et [use&#95;text&#95;index&#95;postings&#95;cache](/fr/operations/settings/settings#use_text_index_postings_cache) pour désactiver, pour les requêtes, la lecture et l’écriture dans chacun de ces caches.

Pour vider les caches, utilisez l’instruction [SYSTEM CLEAR TEXT INDEX CACHES](../../../sql-reference/statements/system#drop-text-index-caches)

Veuillez consulter les paramètres serveur suivants pour configurer les caches.

<div id="caching-tokens">
  #### Paramètres du cache des jetons
</div>

| Paramètre                                                                                                                                           | Description                                                                                                     |
| --------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| [text&#95;index&#95;tokens&#95;cache&#95;policy](/fr/operations/server-configuration-parameters/settings#text_index_tokens_cache_policy)               | Nom de la politique du cache des jetons de l’index de texte.                                                     |
| [text&#95;index&#95;tokens&#95;cache&#95;size](/fr/operations/server-configuration-parameters/settings#text_index_tokens_cache_size)                   | Taille maximale du cache en octets.                                                                             |
| [text&#95;index&#95;tokens&#95;cache&#95;max&#95;entries](/fr/operations/server-configuration-parameters/settings#text_index_tokens_cache_max_entries) | Nombre maximal de jetons désérialisés en cache.                                                                 |
| [text&#95;index&#95;tokens&#95;cache&#95;size&#95;ratio](/fr/operations/server-configuration-parameters/settings#text_index_tokens_cache_size_ratio)   | Taille du segment protégé dans le cache des jetons de l’index de texte, par rapport à la taille totale du cache. |

<div id="caching-header">
  #### Paramètres du cache des en-têtes
</div>

| Paramètre                                                                                                                                           | Description                                                                                                      |
| --------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| [text&#95;index&#95;header&#95;cache&#95;policy](/fr/operations/server-configuration-parameters/settings#text_index_header_cache_policy)               | Nom de la stratégie de cache des en-têtes d’index de texte.                                                      |
| [text&#95;index&#95;header&#95;cache&#95;size](/fr/operations/server-configuration-parameters/settings#text_index_header_cache_size)                   | Taille maximale du cache en octets.                                                                              |
| [text&#95;index&#95;header&#95;cache&#95;max&#95;entries](/fr/operations/server-configuration-parameters/settings#text_index_header_cache_max_entries) | Nombre maximal d’en-têtes désérialisés dans le cache.                                                            |
| [text&#95;index&#95;header&#95;cache&#95;size&#95;ratio](/fr/operations/server-configuration-parameters/settings#text_index_header_cache_size_ratio)   | Taille de la file protégée dans le cache des en-têtes d’index de texte, par rapport à la taille totale du cache. |

<div id="caching-posting-lists">
  #### Paramètres du cache des listes de postings
</div>

| Paramètre                                                                                                                                               | Description                                                                                                         |
| ------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| [text&#95;index&#95;postings&#95;cache&#95;policy](/fr/operations/server-configuration-parameters/settings#text_index_postings_cache_policy)               | Nom de la politique du cache des postings de l’index de texte.                                                      |
| [text&#95;index&#95;postings&#95;cache&#95;size](/fr/operations/server-configuration-parameters/settings#text_index_postings_cache_size)                   | Taille maximale du cache en octets.                                                                                 |
| [text&#95;index&#95;postings&#95;cache&#95;max&#95;entries](/fr/operations/server-configuration-parameters/settings#text_index_postings_cache_max_entries) | Nombre maximal de postings désérialisés dans le cache.                                                              |
| [text&#95;index&#95;postings&#95;cache&#95;size&#95;ratio](/fr/operations/server-configuration-parameters/settings#text_index_postings_cache_size_ratio)   | Taille de la file protégée dans le cache des postings de l’index de texte, par rapport à la taille totale du cache. |

<div id="limitations">
  ## Limitations
</div>

L’index de texte présente actuellement les limitations suivantes :

* La matérialisation des index de texte avec un grand nombre de tokens (par ex. 10 milliards de tokens) peut consommer des quantités importantes de mémoire. La
  matérialisation d’un index de texte peut se produire directement (`ALTER TABLE <table> MATERIALIZE INDEX <index>`) ou indirectement lors des fusions de parties.
* Il n’est pas possible de matérialiser des index de texte sur des parties de plus de 4.294.967.296 (= 2^32 = env. 4,2 milliards) de lignes. Sans index de texte matérialisé, les requêtes se rabattent sur une recherche exhaustive lente dans la partie. Dans le pire des cas, supposez qu’une partie contienne une seule colonne de type String et que le paramètre MergeTree `max_bytes_to_merge_at_max_space_in_pool` (par défaut : 150 GB) n’ait pas été modifié. Dans ce cas, cette situation se produit si la colonne contient en moyenne moins de 29,5 caractères par ligne. En pratique, les tables contiennent aussi d’autres colonnes et le seuil est plusieurs fois plus faible (en fonction du nombre, du type et de la taille des autres colonnes).

<div id="text-index-vs-bloom-filter-indexes">
  ## Index de texte vs index basés sur des filtres de Bloom
</div>

Les prédicats sur les chaînes de caractères peuvent être accélérés à l&#39;aide d&#39;index de texte et d&#39;index basés sur des filtres de Bloom (type d&#39;index `bloom_filter`, `ngrambf_v1`, `tokenbf_v1`, `sparse_grams`), mais leurs conceptions et leurs cas d&#39;usage diffèrent fondamentalement :

**Index à filtre de Bloom**

* Reposent sur des structures de données probabilistes qui peuvent produire des faux positifs.
* Peuvent uniquement répondre à des questions d&#39;appartenance à un ensemble, c.-à-d. indiquer que la colonne peut contenir le token X ou qu&#39;elle ne contient définitivement pas X.
* Stockent des informations au niveau des granules afin de permettre d&#39;ignorer de larges plages lors de l&#39;exécution des requêtes.
* Sont difficiles à paramétrer correctement (voir [ici](mergetree#n-gram-bloom-filter) pour un exemple).
* Sont relativement compacts (quelques kilo-octets ou mégaoctets par partie).

**Index de texte**

* Construisent un index inversé déterministe sur les tokens. L&#39;index lui-même ne peut pas produire de faux positifs.
* Sont spécifiquement optimisés pour les charges de travail de recherche textuelle.
* Stockent des informations au niveau des lignes, ce qui permet une recherche efficace des termes.
* Sont relativement volumineux (de dizaines à centaines de mégaoctets par partie).

Les index basés sur des filtres de Bloom ne prennent en charge la recherche en texte intégral que comme un « effet secondaire » :

* Ils ne prennent pas en charge la tokenisation ni le prétraitement avancés.
* Ils ne prennent pas en charge la recherche sur plusieurs tokens.
* Ils n&#39;offrent pas les caractéristiques de performance attendues d&#39;un index inversé.

Les index de texte, en revanche, sont conçus spécifiquement pour la recherche en texte intégral :

* Ils fournissent la tokenisation et le prétraitement
* Ils prennent efficacement en charge `hasAllTokens`, `LIKE`, `match` et des fonctions de recherche textuelle similaires.
* Ils offrent une bien meilleure scalabilité pour les grands corpus textuels.

<div id="implementation">
  ## Détails d’implémentation
</div>

Chaque index de texte se compose de deux structures de données (abstraites) :

* un dictionnaire qui associe chaque token à une liste de postings, et
* un ensemble de listes de postings, chacune représentant un ensemble de numéros de ligne.

L’index de texte est construit pour l’ensemble de la part.
Contrairement aux autres skip indexes, l’index de texte peut être fusionné au lieu d’être reconstruit lors de la fusion des data parts (voir ci-dessous).

Lors de la création de l’index, trois fichiers sont créés (par part) :

**Fichier des blocs de dictionnaire (.dct)**

Les tokens de l’index de texte sont triés et stockés dans des blocs de dictionnaire de 512 tokens chacun (la taille des blocs est configurable via le paramètre `dictionary_block_size`).
Un fichier de blocs de dictionnaire (.dct) contient tous les blocs de dictionnaire de toutes les index granules d’une part.

**Fichier d’en-tête d’index (.idx)**

Le fichier d’en-tête d’index contient, pour chaque bloc de dictionnaire, le premier token du bloc et son décalage relatif dans le fichier des blocs de dictionnaire.

Cette structure d’index sparse est similaire à l’[index de clé primaire sparse](https://clickhouse.com/docs/guides/best-practices/sparse-primary-indexes)) de ClickHouse.

**Fichier des listes de postings (.pst)**

Les listes de postings de tous les tokens sont stockées séquentiellement dans le fichier des listes de postings.
Pour économiser de l’espace tout en permettant des opérations rapides d’intersection et d’union, les listes de postings sont stockées sous forme de [bitmaps Roaring](https://roaringbitmap.org/).
Si une liste de postings dépasse `posting_list_block_size`, elle est divisée en plusieurs blocs stockés séquentiellement dans le fichier des listes de postings.

**Fichier des positions (.pos)**

Facultatif, uniquement si l’argument d’index `positions = 1`.
Stocke les positions des tokens dans les lignes correspondantes.

**Fusion des index de texte**

Lorsque des data parts sont fusionnées, l’index de texte n’a pas besoin d’être reconstruit de zéro ; il peut être fusionné efficacement dans une étape distincte du merge process.
Pendant cette étape, les dictionnaires triés des index de texte de chaque part d’entrée sont lus et combinés en un nouveau dictionnaire unifié.
Les numéros de ligne dans les listes de postings sont également recalculés pour refléter leurs nouvelles positions dans la data part fusionnée, à l’aide d’une correspondance entre anciens et nouveaux numéros de ligne créée pendant la phase initiale de merge.
Cette méthode de fusion des index de texte est similaire à la façon dont les [projections](/fr/docs/sql-reference/statements/alter/projection#projection-indexes) avec la colonne `_part_offset` sont fusionnées.
Si l’index n’est pas materialized dans la part source, il est construit, écrit dans un fichier temporaire, puis fusionné avec les index des autres parts et des autres fichiers d’index temporaires.

**Débogage**

La table function [mergeTreeTextIndex](../../../sql-reference/table-functions/mergeTreeTextIndex.md) peut être utilisée pour inspecter les index de texte.

<div id="hacker-news-dataset">
  ## Exemple : jeu de données Hacker News
</div>

Examinons les gains de performances des index de texte sur un grand jeu de données contenant beaucoup de texte.
Nous utiliserons 28,7 millions de lignes de commentaires du célèbre site Hacker News.
Voici la table sans index de texte :

```sql
CREATE TABLE hackernews (
    id UInt64,
    deleted UInt8,
    type String,
    author String,
    timestamp DateTime,
    comment String,
    dead UInt8,
    parent UInt64,
    poll UInt64,
    children Array(UInt32),
    url String,
    score UInt32,
    title String,
    parts Array(UInt32),
    descendants UInt32
)
ENGINE = MergeTree
ORDER BY (type, author);
```

Les 28,7 millions de lignes se trouvent dans un fichier Parquet sur S3 ; insérons-les dans la table `hackernews` :

```sql
INSERT INTO hackernews
    SELECT * FROM s3Cluster(
        'default',
        'https://datasets-documentation.s3.eu-west-3.amazonaws.com/hackernews/hacknernews.parquet',
        'Parquet',
        '
    id UInt64,
    deleted UInt8,
    type String,
    by String,
    time DateTime,
    text String,
    dead UInt8,
    parent UInt64,
    poll UInt64,
    kids Array(UInt32),
    url String,
    score UInt32,
    title String,
    parts Array(UInt32),
    descendants UInt32');
```

Nous allons utiliser `ALTER TABLE` pour ajouter un index de texte sur la colonne comment, puis le matérialiser :

```sql
-- Add the index
ALTER TABLE hackernews ADD INDEX comment_idx comment TYPE text(tokenizer = splitByNonAlpha);

-- Materialize the index for existing data
ALTER TABLE hackernews MATERIALIZE INDEX comment_idx SETTINGS mutations_sync = 2;
```

Maintenant, exécutons des requêtes à l’aide des fonctions `hasToken`, `hasAnyTokens` et `hasAllTokens`.
Les exemples suivants montreront la différence de performances spectaculaire entre un scan d’index standard et l’optimisation lecture directe.

<div id="using-hasToken">
  ### 1. Utilisation de `hasToken`
</div>

`hasToken` vérifie si le texte contient un token unique spécifique.
Nous allons rechercher le token sensible à la casse &#39;ClickHouse&#39;.

**Lecture directe désactivée (scan standard)**
Par défaut, ClickHouse utilise le skip index pour filtrer les granules, puis lit les données des colonnes pour ces granules.
Nous pouvons simuler ce comportement en désactivant la lecture directe.

```sql
SELECT count()
FROM hackernews
WHERE hasToken(comment, 'ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 0;

┌─count()─┐
│     516 │
└─────────┘

1 row in set. Elapsed: 0.362 sec. Processed 24.90 million rows, 9.51 GB
```

**Lecture directe activée (lecture rapide de l’index)**
Nous exécutons maintenant la même requête avec la lecture directe activée (option par défaut).

```sql
SELECT count()
FROM hackernews
WHERE hasToken(comment, 'ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 1;

┌─count()─┐
│     516 │
└─────────┘

1 row in set. Elapsed: 0.008 sec. Processed 3.15 million rows, 3.15 MB
```

La requête avec lecture directe est plus de 45 fois plus rapide (0.362s contre 0.008s) et traite nettement moins de données (9.51 GB contre 3.15 MB) en lisant uniquement l’index.

<div id="using-hasAnyTokens">
  ### 2. Utilisation de `hasAnyTokens`
</div>

`hasAnyTokens` vérifie si le texte contient au moins un des tokens spécifiés.
Nous allons rechercher les commentaires contenant soit &#39;love&#39;, soit &#39;ClickHouse&#39;.

**Lecture directe désactivé (Standard scan)**

```sql
SELECT count()
FROM hackernews
WHERE hasAnyTokens(comment, 'love ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 0;

┌─count()─┐
│  408426 │
└─────────┘

1 row in set. Elapsed: 1.329 sec. Processed 28.74 million rows, 9.72 GB
```

**Lecture directe activé (lecture rapide de l’index)**

```sql
SELECT count()
FROM hackernews
WHERE hasAnyTokens(comment, 'love ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 1;

┌─count()─┐
│  408426 │
└─────────┘

1 row in set. Elapsed: 0.015 sec. Processed 27.99 million rows, 27.99 MB
```

L’accélération est encore plus spectaculaire pour cette recherche courante avec l’opérateur &quot;OR&quot;.
La requête est presque 89 fois plus rapide (1.329s vs 0.015s) en évitant un scan complet de la colonne.

<div id="using-hasAllTokens">
  ### 3. Utilisation de `hasAllTokens`
</div>

`hasAllTokens` vérifie si le texte contient tous les tokens spécifiés.
Nous allons rechercher des commentaires contenant à la fois &#39;love&#39; et &#39;ClickHouse&#39;.

**Lecture directe désactivée (Standard scan)**
Même avec lecture directe désactivée, le skip index standard reste efficace.
Il ramène les 28,7 M de lignes à seulement 147,46 K lignes, mais il doit toujours lire 57,03 Mo dans la colonne.

```sql
SELECT count()
FROM hackernews
WHERE hasAllTokens(comment, 'love ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 0;

┌─count()─┐
│      11 │
└─────────┘

1 row in set. Elapsed: 0.184 sec. Processed 147.46 thousand rows, 57.03 MB
```

**Lecture directe activée (lecture rapide de l’index)**
La lecture directe répond à la requête à partir des données de l’index et ne lit que 147.46 KB.

```sql
SELECT count()
FROM hackernews
WHERE hasAllTokens(comment, 'love ClickHouse')
SETTINGS query_plan_direct_read_from_text_index = 1;

┌─count()─┐
│      11 │
└─────────┘

1 row in set. Elapsed: 0.007 sec. Processed 147.46 thousand rows, 147.46 KB
```

Pour cette recherche &quot;AND&quot;, l’optimisation lecture directe est plus de 26 fois plus rapide (0,184 s contre 0,007 s) que le parcours standard du skip index.

<div id="compound-search">
  ### 4. Recherche composée : OR, AND, NOT, ...
</div>

L’optimisation lecture directe s’applique également aux expressions booléennes composées.
Ici, nous allons effectuer une recherche de &#39;ClickHouse&#39; OR &#39;clickhouse&#39; sans tenir compte de la casse.

**Lecture directe désactivée (Standard scan)**

```sql
SELECT count()
FROM hackernews
WHERE hasToken(comment, 'ClickHouse') OR hasToken(comment, 'clickhouse')
SETTINGS query_plan_direct_read_from_text_index = 0;

┌─count()─┐
│     769 │
└─────────┘

1 row in set. Elapsed: 0.450 sec. Processed 25.87 million rows, 9.58 GB
```

**Lecture directe activée (lecture rapide de l’index)**

```sql
SELECT count()
FROM hackernews
WHERE hasToken(comment, 'ClickHouse') OR hasToken(comment, 'clickhouse')
SETTINGS query_plan_direct_read_from_text_index = 1;

┌─count()─┐
│     769 │
└─────────┘

1 row in set. Elapsed: 0.013 sec. Processed 25.87 million rows, 51.73 MB
```

En combinant les résultats de l’index, la requête en lecture directe est 34 fois plus rapide (0,450 s contre 0,013 s) et évite de lire 9,58 Go de données de colonne.
Dans ce cas précis, `hasAnyTokens(comment, ['ClickHouse', 'clickhouse'])` serait la syntaxe à privilégier, car elle est plus efficace.

<div id="related-content">
  ## Contenu connexe
</div>

* Blog : [Annonce de la disponibilité générale de la recherche en texte intégral de ClickHouse](https://clickhouse.com/blog/full-text-search-ga-release)
* Blog : [Concevoir une recherche en texte intégral haute performance pour le stockage d’objets](https://clickhouse.com/blog/clickhouse-full-text-search-object-storage)
* Vidéo : [Introduction à la recherche en texte intégral dans ClickHouse](https://www.youtube.com/watch?v=9zPmf1a_heU)
* Vidéo : [Dans les coulisses : la recherche en texte intégral dans ClickHouse, à grande échelle et à haute vitesse](https://www.youtube.com/watch?v=8JbqE_ubfkU)
* Présentation : [Au cœur de la recherche en texte intégral de ClickHouse : rapide, native et colonnaire](https://github.com/ClickHouse/clickhouse-presentations/blob/master/2025-tumuchdata-munich/ClickHouse_%20full-text%20search%20-%2011.11.2025%20Munich%20Database%20Meetup.pdf)
* Présentation : [Index inversés de bases de données : pourquoi, quoi et comment, FOSDEM 2026](https://presentations.clickhouse.com/2026-fosdem-inverted-index/Inverted_indexes_the_what_the_why_the_how.pdf)

**Contenu obsolète**

* Blog : [Introduction aux index inversés dans ClickHouse](https://clickhouse.com/blog/clickhouse-search-with-inverted-indices)
* Blog : [Au cœur de la recherche en texte intégral de ClickHouse : rapide, native et colonnaire](https://clickhouse.com/blog/clickhouse-full-text-search)
* Vidéo : [Index de texte intégral : conception et expérimentations](https://www.youtube.com/watch?v=O_MnyUkrIq8)