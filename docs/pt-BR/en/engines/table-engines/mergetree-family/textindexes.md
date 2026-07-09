---
description: 'Encontre rapidamente termos de pesquisa em textos.'
keywords: ['pesquisa de texto completo', 'índice de texto', 'índice', 'índices']
sidebar_label: 'Pesquisa de texto completo com índices de texto'
slug: /engines/table-engines/mergetree-family/textindexes
title: 'Pesquisa de texto completo com índices de texto'
doc_type: 'reference'
---

Índices de texto (também conhecidos como [índices invertidos](https://en.wikipedia.org/wiki/Inverted_index)) permitem fazer pesquisas rápidas de texto completo em dados textuais.
Um índice de texto armazena um mapeamento de tokens para os números das linhas que contêm cada token.
Os tokens são gerados por um processo chamado tokenização.
Por exemplo, o tokenizador padrão do ClickHouse converte a frase em inglês &quot;The cat likes mice.&quot; nos tokens [&quot;The&quot;, &quot;cat&quot;, &quot;likes&quot;, &quot;mice&quot;].

Como exemplo, considere uma tabela com uma única coluna e três linhas

```result
1: The cat likes mice.
2: Mice are afraid of dogs.
3: I have two dogs and a cat.
```

Os tokens correspondentes são:

```result
1: The, cat, likes, mice
2: Mice, are, afraid, of, dogs
3: I, have, two, dogs, and, a, cat
```

Em geral, fazemos a pesquisa sem diferenciar maiúsculas de minúsculas; por isso, convertemos os tokens para minúsculas:

```result
1: the, cat, likes, mice
2: mice, are, afraid, of, dogs
3: i, have, two, dogs, and, a, cat
```

Também removeremos palavras de preenchimento, como &quot;I&quot;, &quot;the&quot; e &quot;and&quot;, pois elas aparecem em quase todas as linhas:

```result
1: cat, likes, mice
2: mice, afraid, dogs
3: have, two, dogs, cat
```

Um índice de texto contém então, conceitualmente, estas informações:

```result
afraid : [2]
cat    : [1, 3]
dogs   : [2, 3]
have   : [3]
likes  : [1]
mice   : [1]
two    : [3]
```

Dado um token de busca, essa estrutura de índice permite encontrar rapidamente todas as linhas que correspondem a ele.

<div id="creating-a-text-index">
  ## Criando um índice de texto
</div>

Os índices de texto estão disponíveis de forma geral (GA) no ClickHouse versão 26.2 e posteriores.
Nessas versões, não é necessário configurar nenhuma opção especial para usar o índice de texto.
Recomendamos fortemente o uso do ClickHouse nas versões &gt;= 26.2 em ambientes de produção.

:::note
Os índices de texto podem ser usados com qualquer versão do ClickHouse &gt;= 26.2, independentemente da configuração de [compatibilidade](../../../operations/settings/settings#compatibility).
:::

Para criar um índice de texto, use a seguinte sintaxe:

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

Índices de texto podem ser definidos em colunas dos seguintes tipos:

* [String](/pt-BR/sql-reference/data-types/string.md) e [FixedString](/pt-BR/sql-reference/data-types/fixedstring.md),
* [Array(String)](/pt-BR/sql-reference/data-types/array.md) e [Array(FixedString)](/pt-BR/sql-reference/data-types/array.md),
* [Map](/pt-BR/sql-reference/data-types/map.md) (por meio das funções [mapKeys](/pt-BR/sql-reference/functions/tuple-map-functions.md/#mapKeys) e [mapValues](/pt-BR/sql-reference/functions/tuple-map-functions.md/#mapValues)), e
* [JSON](/pt-BR/sql-reference/data-types/newjson.md) (por meio das funções [JSONAllPaths](/pt-BR/sql-reference/functions/json-functions.md/#JSONAllPaths) e [`JSONAllValues`](/pt-BR/sql-reference/functions/json-functions.md#JSONAllValues)).

Colunas do tipo [Nullable(T)](/pt-BR/sql-reference/data-types/nullable.md) e [LowCardinality()](/pt-BR/sql-reference/data-types/lowcardinality.md) também são suportadas, incluindo `Array(Nullable(String or FixedString))`.

Como alternativa, para adicionar um índice de texto a uma tabela existente:

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

Se você adicionar um índice a uma tabela existente, recomendamos materializar o índice nas partes de tabela existentes (caso contrário, a busca em partes sem índice recorrerá a varreduras lentas por força bruta).

```sql title="Query"
ALTER TABLE table MATERIALIZE INDEX text_idx SETTINGS mutations_sync = 2;
```

Para remover um índice de texto, execute

```sql title="Query"
ALTER TABLE table DROP INDEX text_idx;
```

**Argumento `tokenizer` (obrigatório)**. O argumento `tokenizer` especifica o tokenizador:

* `splitByNonAlpha` divide strings em caracteres ASCII não alfanuméricos (consulte a função [splitByNonAlpha](/pt-BR/sql-reference/functions/splitting-merging-functions.md/#splitByNonAlpha)).
* `splitByString(S)` divide strings com base em determinadas strings separadoras `S` definidas pelo usuário (consulte a função [splitByString](/pt-BR/sql-reference/functions/splitting-merging-functions.md/#splitByString)).
  Os separadores podem ser especificados com um parâmetro opcional, por exemplo, `tokenizer = splitByString([', ', '; ', '\n', '\\'])`.
  Observe que cada string pode ser composta por vários caracteres (`', '` no exemplo).
  A lista padrão de separadores, se não for especificada explicitamente (por exemplo, `tokenizer = splitByString`), é um único espaço em branco `[' ']`.
* `asciiCJK` divide strings em tokens usando regras de delimitação de palavras do Unicode (semelhante a [Unicode Text Segmentation (UAX #29)](https://unicode.org/reports/tr29/)). Caracteres ASCII alfanuméricos e sublinhados formam tokens com conectores (ASCII `:` para letras, `.` e `'` para caracteres do mesmo tipo). Caracteres Unicode não ASCII, incluindo caracteres [CJK](https://en.wikipedia.org/wiki/CJK_characters), tornam-se tokens de um único caractere.
* `ngrams(N)` divide strings em `N`-grams de tamanho igual (consulte a função [ngrams](/pt-BR/sql-reference/functions/splitting-merging-functions.md/#ngrams)).
  O comprimento do ngram pode ser especificado com um parâmetro inteiro opcional entre 1 e 8, por exemplo, `tokenizer = ngrams(3)`.
  O tamanho padrão do ngram, se não for especificado explicitamente (por exemplo, `tokenizer = ngrams`), é 3.
* `sparseGrams(min_length, max_length, min_cutoff_length)` divide strings em n-grams de comprimento variável, com no mínimo `min_length` e no máximo `max_length` caracteres (inclusive) (consulte a função [sparseGrams](/pt-BR/sql-reference/functions/string-functions#sparseGrams)).
  A menos que sejam especificados explicitamente, `min_length` e `max_length` assumem os valores padrão 3 e 100.
  Se o parâmetro `min_cutoff_length` for fornecido, apenas n-grams com comprimento maior ou igual a `min_cutoff_length` serão retornados.
  Em comparação com `ngrams(N)`, o tokenizer `sparseGrams` produz N-grams de comprimento variável, permitindo uma representação mais flexível do texto original.
  Por exemplo, `tokenizer = sparseGrams(3, 5, 4)` gera internamente 3-, 4- e 5-grams a partir da string de entrada, mas retorna apenas os 4- e 5-grams.
* `array` não realiza tokenização, ou seja, cada valor de linha é um token (consulte a função [array](/pt-BR/sql-reference/functions/array-functions.md/#array)).

Todos os tokenizers disponíveis estão listados em [system.tokenizers](../../../operations/system-tables/tokenizers.md).

:::note
O tokenizer `splitByString` aplica os separadores de divisão da esquerda para a direita.
Isso pode criar ambiguidades.
Por exemplo, as strings separadoras `['%21', '%']` farão com que `%21abc` seja tokenizado como `['abc']`, enquanto inverter a ordem dessas strings separadoras para `['%', '%21']` produzirá `['21abc']`.
Na maioria dos casos, convém que a correspondência dê preferência aos separadores mais longos.
Em geral, isso pode ser feito passando as strings separadoras em ordem decrescente de comprimento.
Se as strings separadoras formarem um [prefix code](https://en.wikipedia.org/wiki/Prefix_code), elas podem ser passadas em qualquer ordem.
:::

Para entender como um tokenizer divide a string de entrada, você pode usar as funções [tokens](/pt-BR/sql-reference/functions/splitting-merging-functions.md/#tokens) e [tokensForLikePattern](/pt-BR/sql-reference/functions/splitting-merging-functions.md/#tokensForLikePattern):

Exemplo:

```sql title="Query"
SELECT tokens('abc def', 'ngrams', 3);
```

```result title="Response"
['abc','bc ','c d',' de','def']
```

*Trabalhando com entradas não ASCII.*
Índices de texto podem ser criados com base em dados textuais em qualquer idioma e conjunto de caracteres.
Para texto não ASCII, recomenda-se o tokenizer `asciiCJK`, pois ele lida corretamente com os limites de palavras em Unicode, incluindo caracteres CJK.
:::

**Argumento de pré-processador (opcional)**. O pré-processador se refere a uma expressão aplicada à string de entrada antes da tokenização.

Casos de uso típicos do argumento de pré-processador incluem

1. Conversão para minúsculas/maiúsculas, ou case folding para permitir correspondência sem diferenciar maiúsculas de minúsculas, por exemplo, [lower](/pt-BR/sql-reference/functions/string-functions.md/#lower), [lowerUTF8](/pt-BR/sql-reference/functions/string-functions.md/#lowerUTF8), [caseFoldUTF8](/pt-BR/sql-reference/functions/string-functions.md/#caseFoldUTF8).
2. Normalização UTF-8, por exemplo, [normalizeUTF8NFC](/pt-BR/sql-reference/functions/string-functions.md/#normalizeUTF8NFC), [normalizeUTF8NFD](/pt-BR/sql-reference/functions/string-functions.md/#normalizeUTF8NFD), [normalizeUTF8NFKC](/pt-BR/sql-reference/functions/string-functions.md/#normalizeUTF8NFKC), [normalizeUTF8NFKD](/pt-BR/sql-reference/functions/string-functions.md/#normalizeUTF8NFKD), [normalizeUTF8NFKCCasefold](/pt-BR/sql-reference/functions/string-functions.md/#normalizeUTF8NFKCCasefold), [toValidUTF8](/pt-BR/sql-reference/functions/string-functions.md/#toValidUTF8).
3. Remoção ou transformação de caracteres ou substrings indesejados, como acentos, por exemplo, [extractTextFromHTML](/pt-BR/sql-reference/functions/string-functions.md/#extractTextFromHTML), [substring](/pt-BR/sql-reference/functions/string-functions.md/#substring), [idnaEncode](/pt-BR/sql-reference/functions/string-functions.md/#idnaEncode), [translate](/pt-BR/sql-reference/functions/string-replace-functions.md/#translate), [removeDiacriticsUTF8](/pt-BR/sql-reference/functions/string-functions.md/#removeDiacriticsUTF8).

A expressão do pré-processador deve transformar um valor de entrada do tipo [String](/pt-BR/sql-reference/data-types/string.md) ou [FixedString](/pt-BR/sql-reference/data-types/fixedstring.md) em um valor do mesmo tipo.
Se o índice de texto foi criado em uma coluna do tipo `Nullable(T)` ou `LowCardinality(T)`, então a expressão do pré-processador deve aceitar valores anuláveis ou de baixa cardinalidade (ou seja, não lançar exceção).

Exemplos:

* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(col))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = substringIndex(col, '\n', 1))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(extractTextFromHTML(col)))`
* `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = removeDiacriticsUTF8(caseFoldUTF8(col)))`

Além disso, a expressão do pré-processador deve referenciar apenas a coluna ou expressão sobre a qual o índice de texto está definido.

Exemplos:

* `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = upper(lower(col)))`
* `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(lower(col), lower(col)))`
* Não permitido: `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(col, col))`

O uso de funções não determinísticas não é permitido.

:::note
Em princípio, os pré-processadores são equivalentes a envolver a coluna ou expressão do índice com a expressão do pré-processador.
Por exemplo, o pré-processador `lower` em `INDEX idx col TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(col))` pode ser emulado por `INDEX idx lower(col) TYPE text(tokenizer = 'splitByNonAlpha')`.
A segunda forma tem a desvantagem de que o pré-processador emulado só é aplicado se corresponder à condição de filtro na cláusula WHERE.
Por exemplo, `WHERE hasAllTokens(lower(col), [...])` corresponde, enquanto `WHERE hasAllTokens(col, [...])` não corresponde.
Portanto, para uma melhor experiência de uso, recomendamos usar expressões de pré-processador.
:::

As funções [hasToken](/pt-BR/sql-reference/functions/string-search-functions.md/#hasToken), [hasAllTokens](/pt-BR/sql-reference/functions/string-search-functions.md/#hasAllTokens), [hasAnyTokens](/pt-BR/sql-reference/functions/string-search-functions.md/#hasAnyTokens) e [hasPhrase](/pt-BR/sql-reference/functions/string-search-functions.md/#hasPhrase) usam o pré-processador para primeiro transformar o termo de busca antes de tokenizá-lo.
Observe que, como o pré-processador é aplicado apenas no caminho do índice de texto, os resultados dessas funções podem diferir entre consultas que usam o índice de texto e consultas que não o usam (por exemplo, `SETTINGS use_skip_indexes = 0`).

Por exemplo,

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

equivale a:

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

Neste caso, a expressão do pré-processador transforma individualmente os elementos do array.

Exemplo:

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

Para definir um pré-processador em um índice de texto em colunas do tipo [Map](/pt-BR/sql-reference/data-types/map.md), é preciso decidir se o índice é
construído com base nas chaves ou nos valores do Map.

Exemplo:

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

**Argumento de pós-processador (opcional)**. O pós-processador é uma expressão aplicada a cada token de saída após a tokenização.

Ao contrário do pré-processador, que transforma toda a string de entrada antes de o tokenizer dividi-la em tokens, o pós-processador atua sobre os próprios tokens, um de cada vez.
Esse é o lugar natural para transformações que atuam inerentemente no nível do token.

Casos de uso típicos do argumento de pós-processador incluem:

1. **Filtragem de stop words (tokens extremamente frequentes)**. Tokens muito comuns, como &quot;the&quot;, &quot;a&quot; e &quot;is&quot;, têm pouca relevância para busca e aumentam o índice.
   Você pode usar o pós-processador para descartá-los convertendo-os em tokens vazios — tokens vazios são ignorados, isto é, não são adicionados ao índice.
   Exemplo: `if(str IN ('the', 'a', 'an', 'of', 'in', 'is', 'it'), '', str)`
2. **Remoção de timestamp**. Linhas de log geralmente começam com ou contêm um timestamp estruturado, como `2024-01-15T10:23:45`.
   Indexar tokens de timestamp aumenta o índice com strings que não têm relevância para busca.
   Há duas abordagens complementares para ignorar timestamps:
   * **Abordagem com pós-processador**: use o tokenizer `splitByString` (separação por espaço em branco) para que o timestamp inteiro se torne um único token e, em seguida, use `parseDateTimeOrNull` para detectá-lo e descartá-lo.
     Exemplo: `if(isNull(parseDateTimeOrNull(str, '%Y-%m-%dT%H:%i:%S')), str, '')`
     Para timestamps com offsets de timezone ou segundos fracionários, use `parseDateTimeBestEffortOrNull(str)` sem uma format string explícita.
   * **Abordagem com pré-processador**: remova o timestamp da linha de log completa *antes* da tokenização usando uma regular expression.
     Exemplo: `replaceRegexpAll(str, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} ', '')`
     Isso funciona com qualquer tokenizer e é mais eficiente, pois os caracteres do timestamp nunca são tokenizados.
     As duas abordagens podem ser combinadas: o pré-processador remove o timestamp, enquanto o pós-processador normaliza ou filtra os tokens restantes (por exemplo, lowercase + remoção de palavras de severidade como `ERROR` ou `INFO`).
3. **Stemming**. Mapear cada token para seu radical melhora o recall da busca ao corresponder variantes morfológicas que compartilham a mesma raiz.
   Por exemplo, com stemming em inglês, &quot;running&quot;, &quot;runs&quot; e &quot;run&quot; têm todos o radical &quot;run&quot;, portanto uma consulta por qualquer uma dessas variantes corresponde a todas elas.
   O ClickHouse fornece uma função [stem](/pt-BR/sql-reference/functions/string-functions.md/#stem) integrada para vários idiomas.
   Exemplo: `stem(str, 'en')`
4. **Normalização de maiúsculas/minúsculas**. Converter tokens para minúsculas ou maiúsculas para permitir correspondência sem diferenciar maiúsculas de minúsculas, por exemplo [lower](/pt-BR/sql-reference/functions/string-functions.md/#lower), [lowerUTF8](/pt-BR/sql-reference/functions/string-functions.md/#lowerUTF8).
   Para conversão para minúsculas e maiúsculas, recomendamos usar um pré-processador em vez de um pós-processador.

A expressão do pós-processador transforma tokens do tipo [String](/pt-BR/sql-reference/data-types/string.md) em tokens do mesmo tipo.
Além disso, a expressão do pós-processador deve referenciar apenas a coluna ou expressão sobre a qual o índice de texto está definido.
Quando a coluna é do tipo `Array(String)`, o pós-processador ainda atua sobre tokens individuais como valores `String` simples.

O uso de funções não determinísticas não é permitido.

O pós-processador é aplicado a cada token gerado durante a compilação do índice (para o tokenizer `array`, cada elemento do array é um token). No tempo de consulta, o comportamento depende da função:

* Para `hasToken`, `hasAllTokens`, `hasAnyTokens` e `hasPhrase` (com qualquer tokenizer compatível): o pós-processador é aplicado tanto aos tokens do haystack quanto à needle de busca, permitindo correspondência totalmente normalizada (por exemplo, busca que não diferencia maiúsculas de minúsculas). Para `hasPhrase`, os tokens pós-processados são posicionados de forma densa, portanto, um token descartado pelo pós-processador não deixa lacuna posicional, e a frase ainda corresponde mesmo através dele — por exemplo, com um pós-processador de stop words que descarta `the`, `hasPhrase(col, 'see cat')` corresponde a um documento `see the cat`.
* Para todas as outras funções (`=`, `IN`, `has`, `hasAny`, `hasAll`, `mapContains*`): apenas a needle de busca é pós-processada para a busca da dica de índice; o predicado em nível de linha ainda compara com os valores originais da coluna.

Exemplos:

* Remova stop words usando uma expressão de pós-processador:

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

* Remova as marcas de tempo com uma expressão de pós-processador:

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

* Remova os timestamps usando uma expressão de pré-processamento:

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

* Remova os timestamps usando uma expressão que combina pré-processador e pós-processador:

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

* Faça o stemming dos tokens usando uma expressão de pós-processador:

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

**Suporte a funções**.

Para predicados que consultam o índice de texto, o pré-processador e o pós-processador são aplicados ao valor de busca antes da verificação no nível do grânulo, para que a consulta no índice use os mesmos tokens armazenados durante a criação do índice.
Para a maioria das funções (`=`, `IN`, `startsWith`, `endsWith`, `LIKE`, `mapContains*`), o índice de texto é usado apenas para ignorar blocos de dados irrelevantes; o ClickHouse ainda verifica cada linha restante usando o predicado original sobre os dados originais da coluna.
Para funções de busca de tokens (`hasToken`, `hasAllTokens`, `hasAnyTokens`), o índice de texto é o principal caminho de avaliação: o ClickHouse normaliza o termo buscado usando o mesmo pré-processador, tokenizador e pós-processador aplicados no momento da criação do índice, e usa essa forma normalizada tanto para partes da tabela indexadas quanto não indexadas. Com um pós-processador, os tokens do texto pesquisado também são normalizados em tempo de consulta (para qualquer tokenizador, não apenas `array`), para que ambos os lados da comparação sejam transformados de forma consistente e o resultado não dependa de o índice ser lido diretamente (configuração `query_plan_direct_read_from_text_index`) nem de uma determinada parte ter um índice materializado — por exemplo, habilitando correspondência sem distinção entre maiúsculas e minúsculas para `hasAllTokens(col, ['FOO'])` com um pós-processador `lower`.
Sem `positions`, `hasPhrase` usa o índice apenas como indicação e verifica cada linha restante com o predicado original; além disso, um pós-processador normaliza tanto a frase quanto os tokens do texto pesquisado da mesma forma, para que o resultado seja independente do caminho de leitura, e os tokens descartados pelo pós-processador não prejudiquem a adjacência da frase. Com `positions = 1`, `hasPhrase` usa leituras diretas exatas (ainda aplicando o pós-processador, se houver).
Tokens de busca que o pós-processador mapeia para uma string vazia são ignorados, isto é, tratados como ausentes da frase de busca.

| Função                                                                                      | Suporta um pré-processador                                          | Tokenizadores compatíveis                                | Suporta um pós-processador |
| ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- | -------------------------------------------------------- | -------------------------- |
| `=`                                                                                         | sim                                                                 | todos                                                    | sim                        |
| `IN`                                                                                        | sim                                                                 | todos                                                    | sim                        |
| [hasToken](/pt-BR/sql-reference/functions/string-search-functions.md/#hasToken)                   | sim                                                                 | todos (projetado para `splitByNonAlpha`)                 | sim                        |
| [hasAnyTokens(col, str)](/pt-BR/sql-reference/functions/string-search-functions.md/#hasAnyTokens) | sim                                                                 | todos                                                    | sim                        |
| [hasAllTokens(col, str)](/pt-BR/sql-reference/functions/string-search-functions.md/#hasAllTokens) | sim                                                                 | todos                                                    | sim                        |
| [hasAnyTokens(col, arr)](/pt-BR/sql-reference/functions/string-search-functions.md/#hasAnyTokens) | não (os elementos do array são usados como tokens, sem modificação) | todos                                                    | sim                        |
| [hasAllTokens(col, arr)](/pt-BR/sql-reference/functions/string-search-functions.md/#hasAllTokens) | não (os elementos do array são usados como tokens, sem modificação) | todos                                                    | sim                        |
| [hasPhrase](/pt-BR/sql-reference/functions/string-search-functions.md/#hasPhrase)                 | sim                                                                 | `splitByNonAlpha`, `splitByString`, `ngrams`, `asciiCJK` | sim                        |
| [startsWith](/pt-BR/sql-reference/functions/string-functions.md/#startsWith)                      | sim                                                                 | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | sim                        |
| [endsWith](/pt-BR/sql-reference/functions/string-functions.md/#endsWith)                          | sim                                                                 | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | sim                        |
| [like](/pt-BR/sql-reference/functions/string-search-functions.md/#like)                           | sim¹                                                                | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`¹  | sim¹                       |
| [match](/pt-BR/sql-reference/functions/string-search-functions.md/#match)                         | sim¹                                                                | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`¹  | sim¹                       |
| [ilike](/pt-BR/sql-reference/functions/string-search-functions.md/#like)                          | sim² (`lower`/`upper` apenas)                                       | `splitByNonAlpha`, `array`²                              | não²                       |
| [mapContainsKey](/pt-BR/sql-reference/functions/tuple-map-functions#mapContainsKey)               | sim                                                                 | todos                                                    | sim                        |
| [mapContainsValue](/pt-BR/sql-reference/functions/tuple-map-functions#mapContainsValue)           | sim                                                                 | todos                                                    | sim                        |
| [mapContainsKeyLike](/pt-BR/sql-reference/functions/tuple-map-functions#mapContainsKeyLike)       | sim                                                                 | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | sim                        |
| [mapContainsValueLike](/pt-BR/sql-reference/functions/tuple-map-functions#mapContainsValueLike)   | sim                                                                 | `splitByNonAlpha`, `ngrams`, `sparseGrams`, `asciiCJK`   | sim                        |
| [has](/pt-BR/sql-reference/functions/array-functions.md/#has)                                     | sim                                                                 | `array`                                                  | sim                        |
| [hasAny](/pt-BR/sql-reference/functions/array-functions.md/#hasAny)                               | sim                                                                 | `array`                                                  | sim                        |
| [hasAll](/pt-BR/sql-reference/functions/array-functions.md/#hasAll)                               | sim                                                                 | `array`                                                  | sim                        |

¹ `LIKE` e `match` usam leitura direta como indicação para os tokenizadores listados; caso contrário, fazem fallback para varredura por força bruta.
`LIKE` também oferece suporte a *leitura direta (sem indicação)* (habilitado via `use_text_index_like_evaluation_by_dictionary_scan`) para os tokenizadores `splitByNonAlpha` e `array`, sem pré-processador nem pós-processador.

² `ILIKE` só tem suporte por meio de leitura direta (sem indicação) (`use_text_index_like_evaluation_by_dictionary_scan = 1`, tokenizador `splitByNonAlpha` ou `array`).
Não há fallback para usar o índice como indicação: se a configuração estiver desabilitada ou se o tokenizador não estiver no conjunto compatível, o índice não será usado para `ILIKE`.
O pré-processador, se presente, deve ser `lower` ou `upper`; pós-processadores não têm suporte.

**Experimental: argumento de posições (opcional)**.

O parâmetro experimental `positions` (padrão: `0`) controla se o índice armazena as posições dos tokens.
Quando definido como `1`, o índice também armazena dados posicionais (em um arquivo `.pos`), o que permite correspondência exata de frases por meio de leituras diretas para a função [`hasPhrase`](#functions-example-hasphrase).
Armazenar posições aumenta o tamanho do índice em disco e o custo de gravação, por isso esse recurso é opt-in.
O formato em disco ainda não é estável, portanto esse parâmetro é experimental e pode mudar em um lançamento futuro.
Por isso, criar um índice com `positions = 1` exige que a configuração do MergeTree [`allow_experimental_text_index_positions`](/pt-BR/operations/settings/merge-tree-settings#allow_experimental_text_index_positions) esteja habilitada.
Defina `positions = 0` (o padrão) para manter o armazenamento apenas com posting lists; índices de texto criados sem esse argumento continuam sem posições.

:::warning
Esse argumento é experimental e deve ser usado apenas para testes.
Defina a configuração do MergeTree [`allow_experimental_text_index_positions`](/pt-BR/operations/settings/merge-tree-settings#allow_experimental_text_index_positions) para habilitar o armazenamento de posições.
:::

<details markdown="1">
  <summary>Parâmetros avançados opcionais</summary>

  Os valores padrão dos parâmetros avançados a seguir funcionarão bem em praticamente todas as situações.
  Não recomendamos alterá-los.

  O parâmetro opcional `dictionary_block_size` (padrão: 512) especifica o tamanho dos blocos do dicionário em linhas.

  O parâmetro opcional `dictionary_block_frontcoding_compression` (padrão: 1) especifica se os blocos do dicionário usam front coding como compressão.

  O parâmetro opcional `posting_list_block_size` (padrão: 1048576) especifica o tamanho dos blocos de posting list em linhas.

  O parâmetro opcional `posting_list_codec` (padrão: `none`) especifica o codec da posting list:

  * `none` - as posting lists são armazenadas sem compressão adicional.
  * `bitpacking` - aplica [codificação diferencial (delta)](https://en.wikipedia.org/wiki/Delta_encoding), seguida de [bit-packing](https://dev.to/madhav_baby_giraffe/bit-packing-the-secret-to-optimizing-data-storage-and-transmission-m70) (cada um dentro de blocos de tamanho fixo). Deixa as consultas SELECT mais lentas e não é recomendado no momento.

  Como alternativa, os parâmetros avançados acima podem ser definidos no nível da tabela por meio das configurações correspondentes do MergeTree: [`text_index_dictionary_block_size`](/pt-BR/operations/settings/merge-tree-settings#text_index_dictionary_block_size), [`text_index_dictionary_block_frontcoding_compression`](/pt-BR/operations/settings/merge-tree-settings#text_index_dictionary_block_frontcoding_compression), [`text_index_posting_list_block_size`](/pt-BR/operations/settings/merge-tree-settings#text_index_posting_list_block_size) e [`text_index_posting_list_codec`](/pt-BR/operations/settings/merge-tree-settings#text_index_posting_list_codec).
  Eles se aplicam a cada índice de texto da tabela que não especifica o parâmetro explicitamente.

  O principal caso de uso das configurações no nível da tabela é alterar os parâmetros de índice de uma tabela existente sem remover e recriar o índice de texto em todas as partes da tabela.
  Alterar uma configuração no nível da tabela aplica os novos parâmetros apenas aos índices de texto criados para novas partes; as partes existentes mantêm seu layout atual.

  Um argumento fornecido na definição do índice tem precedência sobre a configuração da tabela, por exemplo:

  ```sql
  CREATE TABLE table(
      s String,
      -- Este índice usa 'bitpacking', substituindo o padrão no nível da tabela abaixo:
      INDEX idx_a s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking'),
      -- Este índice herda 'none' da configuração da tabela:
      INDEX idx_b lower(s) TYPE text(tokenizer = 'splitByNonAlpha'))
  ENGINE = MergeTree()
  ORDER BY tuple()
  SETTINGS text_index_posting_list_codec = 'none';
  ```
</details>

*Granularidade do índice.*
Os índices de texto são implementados no ClickHouse como um tipo de [skip indexes](/pt-BR/engines/table-engines/mergetree-family/mergetree.md/#skip-index-types).
No entanto, diferentemente de outros skip indexes, os índices de texto usam granularidade infinita (100 milhões).
Isso pode ser visto na definição da tabela de um índice de texto.

Exemplo:

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

A grande granularidade do índice garante que o índice de texto seja criado para toda a parte.
Uma granularidade de índice especificada explicitamente é ignorada.

<div id="using-a-text-index">
  ## Usando um índice de texto
</div>

Usar um índice de texto em consultas SELECT é simples, pois funções comuns de busca em strings usam o índice automaticamente.
Se não houver um índice em uma coluna ou em uma parte da tabela, as funções de busca em strings recorrerão a varreduras lentas de força bruta.

:::note
Recomendamos usar as funções `hasAnyTokens` e `hasAllTokens` para pesquisar no índice de texto; consulte [abaixo](#functions-example-hasanytokens-hasalltokens).
Essas funções funcionam com todos os tokenizadores disponíveis e todas as possíveis expressões de pré-processador e pós-processador.
Como as outras funções compatíveis surgiram historicamente antes do índice de texto, em muitos casos elas precisaram manter seu comportamento legado (por exemplo, sem suporte a pré-processador ou pós-processador).
:::

<div id="functions-support">
  ### Funções suportadas
</div>

O índice de texto pode ser usado quando funções de texto são usadas na cláusula `WHERE` ou nas cláusulas `PREWHERE`:

```sql
SELECT [...]
FROM [...]
WHERE string_search_function(column_with_text_index)
```

<div id="functions-example-equals">
  #### `=`
</div>

`=` ([equals](/pt-BR/sql-reference/functions/comparison-functions.md/#equals)) corresponde exatamente a todo o termo de busca informado.

Exemplo:

```sql
SELECT * from table WHERE str = 'Hello';
```

<div id="functions-example-in">
  #### `IN`
</div>

`IN` ([in](/pt-BR/sql-reference/functions/in-functions)) é semelhante a `equals`, mas corresponde a todos os termos de busca.

Exemplo:

```sql
SELECT * from table WHERE str IN ('Hello', 'World');
```

:::note
`NOT IN` (`notIn`) não é suportado pelo índice de texto.
:::

<div id="functions-example-like-match">
  #### `LIKE` e `match`
</div>

:::note
Atualmente, essas funções usam o índice de texto para filtragem apenas se o tokenizador do índice for `splitByNonAlpha`, `ngrams` ou `sparseGrams`.
:::

:::note
`NOT LIKE` (`notLike`) não é suportado pelo índice de texto.
:::

Para usar `LIKE` ([like](/pt-BR/sql-reference/functions/string-search-functions.md/#like)) e a função [match](/pt-BR/sql-reference/functions/string-search-functions.md/#match) com índices de texto, o ClickHouse precisa conseguir extrair tokens completos do termo de busca.
No caso do índice com tokenizador `ngrams`, isso acontece se o comprimento das strings pesquisadas entre caracteres curinga for igual ou maior que o comprimento do ngram.

Exemplo de índice de texto com tokenizador `splitByNonAlpha`:

```sql
SELECT count() FROM table WHERE comment LIKE 'support%';
```

`support` no exemplo pode corresponder a `support`, `supports`, `supporting` etc.
Esse tipo de consulta é uma consulta por substring e não pode ser acelerado por um índice de texto.

Para usar um índice de texto em consultas LIKE, o padrão LIKE deve ser reescrito da seguinte forma:

```sql
SELECT count() FROM table WHERE comment LIKE ' support %'; -- or `% support %`
```

Os espaços à esquerda e à direita de `support` garantem que o termo possa ser extraído como um token.

Felizmente, há um caso especial em que ClickHouse pode aproveitar o índice invertido para acelerar significativamente consultas LIKE.

Consulte a [seção de ajuste de desempenho de LIKE/ILIKE](#like-ilike-queries-perf) para mais detalhes.

<div id="functions-example-multisearchany-multimatchany">
  #### `multiSearchAny` e `multiMatchAny`
</div>

[multiSearchAny](/pt-BR/sql-reference/functions/string-search-functions.md/#multiSearchAny) e sua variante UTF-8 [multiSearchAnyUTF8](/pt-BR/sql-reference/functions/string-search-functions.md/#multiSearchAnyUTF8) verificam se alguma entre várias substrings literais ocorre no haystack, e [multiMatchAny](/pt-BR/sql-reference/functions/string-search-functions.md/#multiMatchAny) verifica se alguma entre várias expressões regulares tem correspondência.
Essas funções usam o índice de texto nas mesmas condições que `LIKE` e `match` (veja acima): o ClickHouse precisa conseguir extrair tokens completos de cada needle, e a lista de needles precisa ser constante.
Um grânulo é lido se algum needle puder estar presente nele.

Para `multiMatchAny`, se um único pattern não puder ser reduzido a um requisito de token (por exemplo, `.*`, que corresponde a qualquer documento), o índice de texto não poderá ser usado e a consulta recorrerá a uma varredura completa.

Assim como em `LIKE` e `match`, a busca por substring e por expressão regular funciona melhor com os tokenizadores `ngrams` e `sparseGrams`.
Esses tokenizadores indexam n-grams de caracteres sobrepostos, de modo que um needle é decomposto em n-grams que estão presentes no índice em qualquer lugar em que o needle ocorra como substring, independentemente de começar ou terminar no meio de uma palavra.
Portanto, um needle pode ser usado como está, desde que tenha pelo menos o tamanho do n-gram.

Exemplo do índice de texto com o tokenizador `ngrams`:

```sql
SELECT count() FROM table WHERE multiSearchAny(comment, ['clickhouse', 'support']);
```

Em contraste, o tokenizador `splitByNonAlpha` indexa apenas tokens completos (palavras inteiras).
Como um termo de busca pode começar ou terminar no meio de uma palavra, o ClickHouse descarta os tokens iniciais e finais de cada termo de busca, de modo que o índice possa descartar grânulos usando apenas tokens completos.
Para fazer com que a busca por substring e por expressão regular use o índice com `splitByNonAlpha`, envolva cada termo de busca com caracteres separadores (como espaços), para que ele forme um ou mais tokens completos.

Exemplo de índice de texto com o tokenizador `splitByNonAlpha`:

```sql
SELECT count() FROM table WHERE multiSearchAny(comment, [' clickhouse ', ' support ']);
```

<div id="functions-example-startswith-endswith">
  #### `startsWith` e `endsWith`
</div>

Assim como `LIKE`, as funções [startsWith](/pt-BR/sql-reference/functions/string-functions.md/#startsWith) e [endsWith](/pt-BR/sql-reference/functions/string-functions.md/#endsWith) só podem usar um índice de texto se for possível extrair tokens completos do termo de busca.
No caso do índice com o tokenizador `ngrams`, isso ocorre se o comprimento das strings buscadas entre curingas for igual ou maior que o comprimento do ngram.
Quando um índice de texto usa um pós-processador, essas funções ainda podem usar o índice no modo Hint se os tokens de dica extraídos permanecerem não vazios após a normalização. Se a normalização remover todos os tokens de dica, o índice não será usado para esse predicado.

Exemplo de índice de texto com o tokenizador `splitByNonAlpha`:

```sql
SELECT count() FROM table WHERE startsWith(comment, 'clickhouse support');
```

No exemplo, apenas `clickhouse` é considerado um token.
`support` não é um token porque pode corresponder a `support`, `supports`, `supporting` etc.

Para encontrar todas as linhas que começam com `clickhouse supports`, termine o padrão de busca com um espaço no final:

```sql
startsWith(comment, 'clickhouse supports ')`
```

Da mesma forma, `endsWith` deve ser usado com um espaço inicial:

```sql
SELECT count() FROM table WHERE endsWith(comment, ' olap engine');
```

<div id="functions-example-hastoken">
  #### `hasToken`
</div>

:::note
`hasToken` tem algumas limitações quando usado em lookups em índices de texto com tokenizadores diferentes de `splitByNonAlpha` e/ou expressões de preprocessor/pós-processador.
Recomendamos usar `hasAnyTokens` e `hasAllTokens`.

As variantes case-insensitive `hasTokenCaseInsensitive` e `hasTokenCaseInsensitiveOrNull` não reconhecem índices de texto — elas sempre fazem uma varredura completa das linhas, mesmo em colunas com índice de texto. Para case-insensitive matching, use um preprocessor ou pós-processador `lower(...)` e combine-o com `hasToken` / `hasAllTokens` / `hasAnyTokens`.
:::

A função [hasToken](/pt-BR/sql-reference/functions/string-search-functions.md/#hasToken) faz correspondência com um único token informado.

Ao contrário das funções mencionadas anteriormente, ela não tokeniza o termo de busca (assume que a entrada é um único token).

Exemplo:

```sql
SELECT count() FROM table WHERE hasToken(comment, 'clickhouse');
```

<div id="functions-example-hasanytokens-hasalltokens">
  #### `hasAnyTokens` and `hasAllTokens`
</div>

As funções [hasAnyTokens](/pt-BR/sql-reference/functions/string-search-functions.md/#hasAnyTokens) e [hasAllTokens](/pt-BR/sql-reference/functions/string-search-functions.md/#hasAllTokens) verificam a correspondência com um ou com todos os tokens fornecidos.

Essas duas funções aceitam os tokens de busca como uma string, que será tokenizada com o mesmo tokenizador usado na coluna de índice, ou como um array de tokens já processados, aos quais não será aplicada nenhuma tokenização antes da busca.
Consulte a documentação da função para mais informações.

Exemplo:

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

A função [hasPhrase](/pt-BR/sql-reference/functions/string-search-functions.md/#hasPhrase) faz correspondência com uma frase: todos os tokens devem aparecer de forma consecutiva e na mesma ordem da string de busca.

Diferentemente de `hasAllTokens`, que exige apenas que todos os tokens estejam presentes em algum ponto, `hasPhrase` exige que eles apareçam em sequência.
A frase de busca é tokenizada com o mesmo tokenizador configurado para a coluna de índice.
Quando o índice de texto usa um pós-processador, a frase de busca também é normalizada antes da consulta ao índice.
Observe que a função exige um dos tokenizadores `splitByNonAlpha`, `splitByString`, `ngrams` ou `asciiCJK`.

Exemplo:

```sql
-- Matches: 'clickhouse' and 'olap' must appear consecutively in that order
SELECT count() FROM table WHERE hasPhrase(comment, 'clickhouse olap');

-- Does NOT match a row containing 'olap clickhouse' (wrong order)
-- Does NOT match a row containing 'clickhouse fast olap' (non-consecutive)
```

<div id="functions-example-has">
  #### `has`
</div>

A função de Array [has](/pt-BR/sql-reference/functions/array-functions#has) verifica a correspondência de um único token no array de strings.

Exemplo:

```sql
SELECT count() FROM table WHERE has(array, 'clickhouse');
```

<div id="functions-example-hasany-hasall">
  #### `hasAny` and `hasAll`
</div>

As funções de array [hasAny](/pt-BR/sql-reference/functions/array-functions#hasAny) e [hasAll](/pt-BR/sql-reference/functions/array-functions#hasAll) verificam se a coluna de array indexada contém alguma ou todas as strings de busca de um conjunto constante.

Exemplo:

```sql
SELECT count() FROM table WHERE hasAny(tags, ['clickhouse', 'olap']);
SELECT count() FROM table WHERE hasAll(tags, ['clickhouse', 'olap']);
```

<div id="functions-example-mapcontains">
  #### `mapContains`
</div>

A função [mapContains](/pt-BR/sql-reference/functions/tuple-map-functions#mapContainsKey) (um alias de `mapContainsKey`) faz correspondência com os tokens extraídos da string pesquisada nas chaves de um map.
O comportamento é semelhante ao da função `equals` em uma coluna `String`.
O índice de texto só é usado se tiver sido criado em uma expressão `mapKeys(map)`.

Exemplo:

```sql
SELECT count() FROM table WHERE mapContainsKey(map, 'clickhouse');
-- OR
SELECT count() FROM table WHERE mapContains(map, 'clickhouse');
```

<div id="functions-example-mapcontainsvalue">
  #### `mapContainsValue`
</div>

A função [mapContainsValue](/pt-BR/sql-reference/functions/tuple-map-functions#mapContainsValue) faz a correspondência com tokens extraídos da string pesquisada nos valores de um map.
O comportamento é semelhante ao da função `equals` em uma coluna `String`.
O índice de texto só é usado se tiver sido criado em uma expressão `mapValues(map)`.

Exemplo:

```sql
SELECT count() FROM table WHERE mapContainsValue(map, 'clickhouse');
```

<div id="functions-example-mapcontainslike">
  #### `mapContainsKeyLike` and `mapContainsValueLike`
</div>

As funções [mapContainsKeyLike](/pt-BR/sql-reference/functions/tuple-map-functions#mapContainsKeyLike) e [mapContainsValueLike](/pt-BR/sql-reference/functions/tuple-map-functions#mapContainsValueLike) comparam um padrão com todas as chaves ou valores (respectivamente) de um map.

Exemplo:

```sql
SELECT count() FROM table WHERE mapContainsKeyLike(map, '% clickhouse %');
SELECT count() FROM table WHERE mapContainsValueLike(map, '% clickhouse %');
```

<div id="functions-example-access-operator">
  #### `operator[]`
</div>

O [operator[]](/pt-BR/sql-reference/operators#access-operators) de acesso pode ser usado com o índice de texto para filtrar chaves e valores. O índice de texto só é usado se tiver sido criado nas expressões `mapKeys(map)` ou `mapValues(map)`, ou em ambas.

Exemplo:

```sql
SELECT count() FROM table WHERE map['engine'] = 'clickhouse';
```

Veja os exemplos a seguir de como usar colunas do tipo `Array(T)` e `Map(K, V)` com o índice de texto.

<div id="text-index-example-array">
  ### Indexação de colunas Array(String)
</div>

Imagine uma plataforma de blogs em que os autores categorizam suas publicações usando palavras-chave.
Queremos que os usuários descubram conteúdo relacionado pesquisando ou clicando em tópicos.

Considere esta definição de tabela:

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

Sem um índice de texto, encontrar posts com uma palavra-chave específica (por exemplo, `clickhouse`) exige percorrer todos os registros:

```sql
SELECT count() FROM posts WHERE has(keywords, 'clickhouse'); -- slow full-table scan - checks every keyword in every post
```

À medida que a plataforma cresce, isso fica cada vez mais lento, porque a consulta precisa examinar cada array de `keywords` em cada linha.
Para contornar esse problema de desempenho, definimos um índice de texto para a coluna `keywords`:

```sql
ALTER TABLE posts ADD INDEX keywords_idx(keywords) TYPE text(tokenizer = splitByNonAlpha);
ALTER TABLE posts MATERIALIZE INDEX keywords_idx; -- Don't forget to rebuild the index for existing data
```

<div id="text-index-example-map">
  ### Indexação de colunas map
</div>

Em muitos casos de uso de observabilidade, as mensagens de log são divididas em &quot;componentes&quot; e armazenadas nos tipos de dados adequados, por exemplo, data e hora para o timestamp, enum para o nível de log etc.
Os campos de métricas são mais bem armazenados como pares chave-valor.
As equipes de operações precisam pesquisar logs com eficiência para depuração, incidentes de segurança e monitoramento.

Considere esta tabela de logs:

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

Sem um índice de texto, pesquisar dados [Map](/pt-BR/sql-reference/data-types/map.md) exige varreduras completas da tabela:

```sql
-- Finds all logs with rate limiting data:
SELECT * FROM logs WHERE has(mapKeys(attributes), 'rate_limit'); -- slow full-table scan

-- Finds all logs from a specific IP:
SELECT * FROM logs WHERE has(mapValues(attributes), '192.168.1.1'); -- slow full-table scan
```

À medida que o volume de logs cresce, essas consultas ficam lentas.

A solução é criar um índice de texto para as chaves e os valores do tipo [Map](/pt-BR/sql-reference/data-types/map.md).
Use [mapKeys](/pt-BR/sql-reference/functions/tuple-map-functions.md/#mapKeys) para criar um índice de texto quando precisar encontrar logs por nomes de campos ou tipos de atributo:

```sql
ALTER TABLE logs ADD INDEX attributes_keys_idx mapKeys(attributes) TYPE text(tokenizer = array);
ALTER TABLE posts MATERIALIZE INDEX attributes_keys_idx;
```

Use [mapValues](/pt-BR/sql-reference/functions/tuple-map-functions.md/#mapValues) para criar um índice de texto quando precisar pesquisar no conteúdo real dos atributos:

```sql
ALTER TABLE logs ADD INDEX attributes_vals_idx mapValues(attributes) TYPE text(tokenizer = array);
ALTER TABLE posts MATERIALIZE INDEX attributes_vals_idx;
```

Exemplos de consultas:

```sql
-- Find all rate-limited requests:
SELECT * FROM logs WHERE mapContainsKey(attributes, 'rate_limit'); -- fast

-- Finds all logs from a specific IP:
SELECT * FROM logs WHERE has(mapValues(attributes), '192.168.1.1'); -- fast

-- Finds all logs where any attribute includes an error:
SELECT * FROM logs WHERE mapContainsValueLike(attributes, '% error %'); -- fast
```

<div id="text-index-example-json">
  ### Indexação de colunas JSON
</div>

Índices de texto podem ser usados com colunas `JSON` de três maneiras:

1. **Índices em subcolunas específicas** — crie um índice de texto em um caminho JSON conhecido, assim como em uma coluna comum. Isso indexa os *valores* nesse caminho.
2. **Índices baseados em caminhos com [JSONAllPaths](/pt-BR/sql-reference/functions/json-functions.md/#JSONAllPaths)** — indexam *todos os caminhos* presentes em cada grânulo para ignorar grânulos que não podem conter o caminho consultado. Semelhante ao que ocorre com colunas `Map`.
3. **Índices baseados em valores com [JSONAllValues](/pt-BR/sql-reference/functions/json-functions.md#JSONAllValues)** — indexam *todos os valores* em todos os caminhos JSON para acelerar a busca em texto completo em qualquer subcoluna JSON com um único índice.

<div id="json-indexes-on-subcolumns">
  #### Índices em subcolunas específicas
</div>

Você pode criar um skip index em qualquer subcoluna JSON usando a mesma sintaxe das colunas comuns.

Há duas maneiras de referenciar uma subcoluna JSON em uma expressão de índice:

* **Caminho tipado** declarado na indicação de tipo JSON — acesse-o diretamente pelo nome: `json.a`.
* **Caminho dinâmico** com conversão explícita — use a sintaxe de cast `::`: `json.b::String`.

Exemplo de definição de índice:

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

Exemplo de consulta:

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

Exemplo de consulta:

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
  #### Índices baseados em caminhos com JSONAllPaths
</div>

Assim como nas colunas `Map`, índices de texto podem ser criados em colunas [JSON](/pt-BR/sql-reference/data-types/newjson.md) usando [`JSONAllPaths`](/pt-BR/sql-reference/functions/json-functions.md/#JSONAllPaths).
O índice armazena o conjunto de caminhos JSON presentes em cada grânulo e os utiliza para ignorar grânulos em que o caminho consultado não está presente.

Exemplo de definição de índice:

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

Você pode usar `EXPLAIN indexes = 1` para verificar se o skip index está sendo usado.
Quando um caminho existe apenas em uma das partes, o índice ignora a outra.

Exemplo:

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

Quando um caminho não existe em nenhuma parte, todas as partes e todos os grânulos são ignorados.

Exemplo:

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

`IS NOT NULL` também usa o índice — ele pula os grânulos em que o caminho está ausente (já que o valor seria `NULL`):

Exemplo:

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
  #### Índices baseados em valores com JSONAllValues
</div>

Índices de texto podem ser usados para acelerar buscas em colunas [JSON](/pt-BR/sql-reference/data-types/newjson.md) por meio da função [`JSONAllValues`](/pt-BR/sql-reference/functions/json-functions.md#JSONAllValues).

`JSONAllValues` retorna todos os valores de uma coluna JSON como `Array(String)`.
Valores de tipos de dados que não são string (por exemplo, inteiros e arrays) são convertidos para sua representação textual.
Um índice de texto criado com `JSONAllValues` indexa essas representações textuais em todos os caminhos JSON de cada linha.
Esse índice pode então acelerar consultas que filtram subcolunas JSON específicas.
Quando uma consulta filtra uma subcoluna específica (por exemplo, `data.user_name = 'alice'`), o índice de texto pode rapidamente ignorar linhas (e grânulos) que não contenham os tokens de busca em nenhum dos seus valores JSON.

:::note
O índice pode produzir falsos positivos quando diferentes caminhos JSON contêm os mesmos tokens.
Por exemplo, se a linha 1 tiver `{"a": "hello", "b": "world"}` e uma consulta buscar `data.a = 'world'`, o índice de texto não consegue distinguir que `world` pertence ao caminho `b`, e não a `a`.
Nesses casos, o índice não ignorará a linha, e o filtro sobre os dados reais da coluna fará a avaliação final.
Esse é o mesmo comportamento de outros casos de uso de índices de texto, nos quais o índice atua como um pré-filtro rápido.
:::

<div id="json-all-values-creating-the-index">
  ##### Criando o índice
</div>

Exemplo de definição de índice:

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
  ##### Padrões de consulta compatíveis
</div>

Depois de criado, o índice pode acelerar consultas em subcolunas JSON usando as mesmas funções usadas para colunas `String` e a função `equals` para todas as colunas.

Acesso à subcoluna:

```sql
SELECT * FROM events WHERE data.user_name = 'alice';
SELECT * FROM events WHERE data.message LIKE '% error %';
SELECT * FROM events WHERE startsWith(data.status, 'fail');
SELECT * FROM events WHERE hasToken(data.title, 'clickhouse');
```

Acesso à subcoluna com `CAST` explícito:

```sql
SELECT * FROM events WHERE hasAllTokens(data.message::String, 'connection timeout');
SELECT * FROM events WHERE data.status_code::UInt64 = 404;
SELECT * FROM events WHERE has(data.tags::Array(String), 'bug')
```

operador `IN`:

```sql
SELECT * FROM events WHERE data.level IN ('error', 'critical');
```

<div id="text-index-phrase-search">
  ### Busca por frase
</div>

Por exemplo, uma busca comum em um índice de texto

```sql
SELECT *
FROM tab
WHERE hasAllTokens(col, 'weather in Tokyo')
```

corresponde a todas as linhas que contêm os tokens fornecidos em qualquer ordem.
No exemplo, a linha `While she stayed in Tokyo, the weather was great.` corresponde ao filtro.

Em contraste, a busca por frase significa corresponder aos tokens na ordem especificada.
Por exemplo,

```sql
SELECT *
FROM tab
WHERE hasPhrase(col, 'weather in Tokyo')
```

corresponde a qualquer linha que contenha a sequência de tokens `weather in Tokyo`, como `How is the weather in Tokyo?`?

O índice de texto acelera a busca por frases fazendo a interseção das posting lists de todos os tokens da frase para identificar os grânulos candidatos.
Dentro desses grânulos, o ClickHouse verifica então a adjacência exata dos tokens.
Esse processo é relativamente custoso e mais lento do que consultas regulares de busca textual.
Para acelerar as consultas de busca por frases, habilite o armazenamento de posições no índice de texto (veja `Parâmetros opcionais` acima).

`hasPhrase` pode ser usado junto com os tokenizadores `splitByNonAlpha`, `splitByString`, `ngrams` e `asciiCJK`.
A string da frase fornecida é tokenizada usando o tokenizador do índice.
Os caracteres separadores na frase são ignorados: `hasPhrase(text, 'quick+brown')` é equivalente a `hasPhrase(text, 'quick brown')`, desde que `splitByNonAlpha` seja usado como tokenizador.

<div id="text-index-phrase-search-example">
  #### Exemplo
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

Linha 2 (`'New weather in York'`) não corresponde porque os tokens estão na ordem incorreta.
Linha 3 (`'weather in New Orleans'`) não corresponde porque não contém o token `'York'`.

<div id="performance-tuning">
  ## Otimização de desempenho
</div>

<div id="direct-read">
  ### Leitura direta
</div>

Certos tipos de consultas de texto podem ser significativamente acelerados com uma otimização chamada &quot;leitura direta&quot;.

Exemplo:

```sql
SELECT column_a, column_b, ...
FROM [...]
WHERE string_search_function(column_with_text_index)
```

A otimização de leitura direta responde à consulta exclusivamente usando o índice de texto (ou seja, consultas ao índice de texto), sem acessar a coluna de texto subjacente.
As consultas ao índice de texto leem relativamente poucos dados e, por isso, são muito mais rápidas do que os skip indexes usuais no ClickHouse (que fazem uma consulta ao skip index, seguida do carregamento e da filtragem dos grânulos restantes).

A leitura direta é controlada por duas configurações:

* A configuração [query&#95;plan&#95;direct&#95;read&#95;from&#95;text&#95;index](../../../operations/settings/settings#query_plan_direct_read_from_text_index) (`true` por padrão) especifica se a leitura direta está habilitada de modo geral.
* A configuração [use&#95;skip&#95;indexes&#95;on&#95;data&#95;read](../../../operations/settings/settings#use_skip_indexes_on_data_read) era um pré-requisito para a leitura direta em versões do ClickHouse &lt; 26.4.

**Funções compatíveis**

A otimização de leitura direta oferece suporte às funções `hasToken`, `hasAllTokens` e `hasAnyTokens`.
Se o índice de texto for definido com um tokenizer `array`, a leitura direta também terá suporte para as funções `equals`, `has`, `hasAny`, `hasAll`, `mapContainsKey` e `mapContainsValue`.
Essas funções também podem ser combinadas com os operadores `AND`, `OR` e `NOT`.
As cláusulas `WHERE` ou `PREWHERE` também podem conter filtros adicionais de funções que não sejam de busca de texto (para colunas de texto ou outras colunas) — nesse caso, a otimização de leitura direta ainda será usada, mas será menos eficaz (ela se aplica apenas às funções de busca de texto compatíveis).

Para verificar se uma consulta utiliza leitura direta, execute a consulta com `EXPLAIN PLAN actions = 1`.
Como exemplo, uma consulta com a leitura direta desabilitada

```sql
EXPLAIN PLAN actions = 1
SELECT count()
FROM table
WHERE hasToken(col, 'some_token')
SETTINGS query_plan_direct_read_from_text_index = 0, -- disable direct read
```

retorno

```text
[...]
Filter ((WHERE + Change column names to column identifiers))
Filter column: hasToken(__table1.col, 'some_token'_String) (removed)
Actions: INPUT : 0 -> col String : 0
         COLUMN Const(String) -> 'some_token'_String String : 1
         FUNCTION hasToken(col :: 0, 'some_token'_String :: 1) -> hasToken(__table1.col, 'some_token'_String) UInt8 : 2
[...]
```

enquanto a mesma consulta é executada com `query_plan_direct_read_from_text_index = 1`

```sql
EXPLAIN PLAN actions = 1
SELECT count()
FROM table
WHERE hasToken(col, 'some_token')
SETTINGS query_plan_direct_read_from_text_index = 1, -- enable direct read
```

retorna

```text
[...]
Expression (Before GROUP BY)
Positions:
  Filter
  Filter column: __text_index_idx_hasToken_94cc2a813036b453d84b6fb344a63ad3 (removed)
  Actions: INPUT :: 0 -> __text_index_idx_hasToken_94cc2a813036b453d84b6fb344a63ad3 UInt8 : 0
[...]
```

A segunda saída de EXPLAIN PLAN contém uma coluna virtual `__text_index_<index_name>_<function_name>_<id>`.
Se essa coluna estiver presente, então a leitura direta será usada.

Se a cláusula de filtro WHERE contiver apenas funções de busca de texto, a consulta poderá evitar por completo a leitura dos dados da coluna e obter o maior ganho de desempenho com leitura direta.
No entanto, mesmo que a coluna de texto seja acessada em outra parte da consulta, a leitura direta ainda proporcionará melhoria de desempenho.

**Leitura direta como dica**

A leitura direta como dica se baseia nos mesmos princípios da leitura direta normal, mas, em vez disso, adiciona um filtro extra construído a partir dos dados do índice de texto sem remover a coluna de texto subjacente.
Ele é usado para funções em que ler apenas do índice de texto produziria falsos positivos.

As funções compatíveis são: `like`, `startsWith`, `endsWith`, `equals`, `has`, `hasPhrase`, `mapContainsKey` e `mapContainsValue`.

O filtro adicional pode oferecer seletividade extra para restringir ainda mais o conjunto de resultados em combinação com outros filtros, ajudando a reduzir a quantidade de dados lidos de outras colunas.

A leitura direta como dica é controlada pela configuração [query&#95;plan&#95;text&#95;index&#95;add&#95;hint](../../../operations/settings/settings#query_plan_text_index_add_hint) (habilitada por padrão).

Exemplo de consulta sem dica:

```sql
EXPLAIN actions = 1
SELECT count()
FROM table
WHERE (col LIKE '%some-token%') AND (d >= today())
SETTINGS query_plan_text_index_add_hint = 0
FORMAT TSV
```

retorna

```text
[...]
Prewhere filter column: and(like(__table1.col, \'%some-token%\'_String), greaterOrEquals(__table1.d, _CAST(20440_Date, \'Date\'_String))) (removed)
[...]
```

enquanto a mesma consulta é executada com `query_plan_text_index_add_hint = 1`

```sql
EXPLAIN actions = 1
SELECT count()
FROM table
WHERE col LIKE '%some-token%'
SETTINGS query_plan_text_index_add_hint = 1
```

Retorno

```text
[...]
Prewhere filter column: and(__text_index_idx_col_like_d306f7c9c95238594618ac23eb7a3f74, like(__table1.col, \'%some-token%\'_String), greaterOrEquals(__table1.d, _CAST(20440_Date, \'Date\'_String))) (removed)
[...]
```

Na segunda saída de EXPLAIN PLAN, você pode ver que uma conjunção adicional (`__text_index_...`) foi adicionada à condição de filtragem.
Graças à otimização [PREWHERE](/pt-BR/sql-reference/statements/select/prewhere), a condição de filtragem é dividida em três conjunções separadas, aplicadas em ordem crescente de complexidade computacional.
Para esta consulta, a ordem de aplicação é `__text_index_...`, depois `greaterOrEquals(...)` e, por fim, `like(...)`.
Essa ordenação permite pular ainda mais grânulos de dados do que os já ignorados pelo índice de texto e pelo filtro original, antes da leitura das colunas pesadas usadas na consulta após a cláusula `WHERE`, reduzindo ainda mais a quantidade de dados a ser lida.

<div id="like-ilike-queries-perf">
  ### Consultas LIKE/ILIKE
</div>

Quando o padrão de uma consulta LIKE/ILIKE é `%<alpha-numeric-characters-without-spaces>%` e o tokenizer do índice de texto é `splitByNonAlpha` ou `array`, o ClickHouse usa o índice invertido para acelerar significativamente as consultas LIKE/ILIKE. Para isso, o ClickHouse varre o Dicionário do índice invertido em vez de fazer uma varredura completa da tabela para encontrar o padrão correspondente.

Quando a otimização está habilitada, as consultas LIKE/ILIKE devem ser significativamente mais rápidas do que uma varredura completa da tabela. No entanto, quando o padrão corresponde à maioria dos tokens do dicionário, o desempenho pode ser pior do que em uma varredura completa da tabela. Felizmente, há um mecanismo de fallback para evitar isso.

A otimização é controlada por uma configuração:

* [use&#95;text&#95;index&#95;like&#95;evaluation&#95;by&#95;dictionary&#95;scan](../../../operations/settings/settings#use_text_index_like_evaluation_by_dictionary_scan)

O mecanismo de fallback é controlado por duas configurações:

* [text&#95;index&#95;like&#95;min&#95;pattern&#95;length](../../../operations/settings/settings#text_index_like_min_pattern_length)
* [text&#95;index&#95;like&#95;max&#95;postings&#95;to&#95;read](../../../operations/settings/settings#text_index_like_max_postings_to_read)

Esta otimização oferece suporte apenas às funções `like` e `ilike`.

<div id="caching">
  ### Cache
</div>

Existem diferentes caches no servidor para armazenar em memória partes do índice de texto (consulte a seção [Detalhes de implementação](#implementation)):
Atualmente, há caches para os cabeçalhos desserializados, tokens e listas de postings do índice de texto, para reduzir a E/S.
Use as configurações [use&#95;text&#95;index&#95;header&#95;cache](/pt-BR/operations/settings/settings#use_text_index_header_cache), [use&#95;text&#95;index&#95;tokens&#95;cache](/pt-BR/operations/settings/settings#use_text_index_tokens_cache) e [use&#95;text&#95;index&#95;postings&#95;cache](/pt-BR/operations/settings/settings#use_text_index_postings_cache) para desativar, nas consultas, a leitura e a gravação em cada cache individual.

Para limpar os caches, use a instrução [SYSTEM CLEAR TEXT INDEX CACHES](../../../sql-reference/statements/system#drop-text-index-caches)

Consulte as configurações do servidor a seguir para configurar os caches.

<div id="caching-tokens">
  #### Configurações do cache de tokens
</div>

| Configuração                                                                                                                                        | Descrição                                                                                             |
| --------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| [text&#95;index&#95;tokens&#95;cache&#95;policy](/pt-BR/operations/server-configuration-parameters/settings#text_index_tokens_cache_policy)               | Nome da política de cache usada pelo cache de tokens do índice de texto.                              |
| [text&#95;index&#95;tokens&#95;cache&#95;size](/pt-BR/operations/server-configuration-parameters/settings#text_index_tokens_cache_size)                   | Tamanho máximo do cache em bytes.                                                                     |
| [text&#95;index&#95;tokens&#95;cache&#95;max&#95;entries](/pt-BR/operations/server-configuration-parameters/settings#text_index_tokens_cache_max_entries) | Número máximo de tokens desserializados no cache.                                                     |
| [text&#95;index&#95;tokens&#95;cache&#95;size&#95;ratio](/pt-BR/operations/server-configuration-parameters/settings#text_index_tokens_cache_size_ratio)   | Tamanho da fila protegida no cache de tokens do índice de texto em relação ao tamanho total do cache. |

<div id="caching-header">
  #### Configurações do cache de cabeçalho
</div>

| Configuração                                                                                                                                        | Descrição                                                                                                |
| --------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| [text&#95;index&#95;header&#95;cache&#95;policy](/pt-BR/operations/server-configuration-parameters/settings#text_index_header_cache_policy)               | Nome da política do cache de cabeçalho do índice de texto.                                               |
| [text&#95;index&#95;header&#95;cache&#95;size](/pt-BR/operations/server-configuration-parameters/settings#text_index_header_cache_size)                   | Tamanho máximo do cache em bytes.                                                                        |
| [text&#95;index&#95;header&#95;cache&#95;max&#95;entries](/pt-BR/operations/server-configuration-parameters/settings#text_index_header_cache_max_entries) | Número máximo de cabeçalhos desserializados no cache.                                                    |
| [text&#95;index&#95;header&#95;cache&#95;size&#95;ratio](/pt-BR/operations/server-configuration-parameters/settings#text_index_header_cache_size_ratio)   | Tamanho da fila protegida no cache de cabeçalho do índice de texto em relação ao tamanho total do cache. |

<div id="caching-posting-lists">
  #### Configurações do cache de listas de postings
</div>

| Configuração                                                                                                                                            | Descrição                                                                                               |
| ------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| [text&#95;index&#95;postings&#95;cache&#95;policy](/pt-BR/operations/server-configuration-parameters/settings#text_index_postings_cache_policy)               | Nome da política de cache das postings do índice de texto.                                              |
| [text&#95;index&#95;postings&#95;cache&#95;size](/pt-BR/operations/server-configuration-parameters/settings#text_index_postings_cache_size)                   | Tamanho máximo do cache em bytes.                                                                       |
| [text&#95;index&#95;postings&#95;cache&#95;max&#95;entries](/pt-BR/operations/server-configuration-parameters/settings#text_index_postings_cache_max_entries) | Número máximo de postings desserializadas no cache.                                                     |
| [text&#95;index&#95;postings&#95;cache&#95;size&#95;ratio](/pt-BR/operations/server-configuration-parameters/settings#text_index_postings_cache_size_ratio)   | Tamanho da fila protegida no cache de postings do índice de texto em relação ao tamanho total do cache. |

<div id="limitations">
  ## Limitações
</div>

Atualmente, o índice de texto tem as seguintes limitações:

* A materialização de índices de texto com um grande número de tokens (por exemplo, 10 bilhões de tokens) pode consumir quantidades significativas de memória. A materialização de
  índices de texto pode ocorrer diretamente (`ALTER TABLE <table> MATERIALIZE INDEX <index>`) ou indiretamente durante mesclagens de partes.
* Não é possível materializar índices de texto em partes com mais de 4.294.967.296 (= 2^32 = aprox. 4,2 bilhões) linhas. Sem um índice de texto materializado, as consultas passam a usar uma busca lenta por força bruta dentro da parte. Como estimativa de pior caso, suponha que uma parte contenha uma única coluna do tipo String e que a configuração do MergeTree `max_bytes_to_merge_at_max_space_in_pool` (padrão: 150 GB) não tenha sido alterada. Nesse caso, isso ocorre se a coluna contiver, em média, menos de 29,5 caracteres por linha. Na prática, as tabelas também contêm outras colunas, e esse limite é várias vezes menor (dependendo do número, tipo e tamanho das outras colunas).

<div id="text-index-vs-bloom-filter-indexes">
  ## Índices de texto vs. índices baseados em filtro de Bloom
</div>

Predicados sobre strings podem ser acelerados com índices de texto e índices baseados em filtro de Bloom (tipo de índice `bloom_filter`, `ngrambf_v1`, `tokenbf_v1`, `sparse_grams`), mas ambos são fundamentalmente diferentes em seu design e nos casos de uso a que se destinam:

**Índices de filtro de Bloom**

* São baseados em estruturas de dados probabilísticas que podem produzir falsos positivos.
* Só conseguem responder a perguntas de pertencimento a conjuntos, ou seja, se a coluna pode conter o token X ou se definitivamente não o contém.
* Armazenam informações no nível de granule para permitir pular intervalos amplos durante a execução da consulta.
* São difíceis de ajustar corretamente (veja [aqui](mergetree#n-gram-bloom-filter) um exemplo).
* São relativamente compactos (alguns kilobytes ou megabytes por parte).

**Índices de texto**

* Constroem um índice invertido determinístico sobre tokens. O próprio índice não pode gerar falsos positivos.
* São especificamente otimizados para cargas de trabalho de busca textual.
* Armazenam informações no nível de linha, o que permite a busca eficiente de termos.
* São relativamente grandes (de dezenas a centenas de megabytes por parte).

Índices baseados em filtro de Bloom oferecem suporte à busca de texto completo apenas como um &quot;efeito colateral&quot;:

* Eles não oferecem suporte a tokenização e preprocessamento avançados.
* Eles não oferecem suporte à busca com múltiplos tokens.
* Eles não fornecem as características de desempenho esperadas de um índice invertido.

Índices de texto, em contraste, são projetados especificamente para busca de texto completo:

* Eles oferecem tokenização e preprocessamento
* Eles oferecem suporte eficiente a `hasAllTokens`, `LIKE`, `match` e funções semelhantes de busca textual.
* Eles têm escalabilidade significativamente melhor para grandes corpus de texto.

<div id="implementation">
  ## Detalhes de implementação
</div>

Cada índice de texto consiste em duas estruturas de dados (abstratas):

* um dicionário que mapeia cada token para uma lista de postings, e
* um conjunto de listas de postings, cada uma representando um conjunto de números de linhas.

O índice de texto é criado para toda a parte.
Ao contrário de outros skip indexes, o índice de texto pode ser mesclado em vez de reconstruído durante a mesclagem das partes de dados (veja abaixo).

Durante a criação do índice, três arquivos são criados (por parte):

**Arquivo de blocos do dicionário (.dct)**

Os tokens no índice de texto são ordenados e armazenados em blocos de dicionário de 512 tokens cada (o tamanho do bloco é configurável pelo parâmetro `dictionary_block_size`).
Um arquivo de blocos do dicionário (.dct) é composto por todos os blocos de dicionário de todos os grânulos de índice em uma parte.

**Arquivo de cabeçalho do índice (.idx)**

O arquivo de cabeçalho do índice contém, para cada bloco de dicionário, o primeiro token do bloco e seu deslocamento relativo no arquivo de blocos do dicionário.

Essa estrutura de índice esparso é semelhante ao [índice esparso de chave primária](https://clickhouse.com/docs/guides/best-practices/sparse-primary-indexes)) do ClickHouse.

**Arquivo de listas de postings (.pst)**

As listas de postings de todos os tokens são dispostas sequencialmente no arquivo de listas de postings.
Para economizar espaço e ainda permitir operações rápidas de interseção e união, as listas de postings são armazenadas como [roaring bitmaps](https://roaringbitmap.org/).
Se a lista de postings for maior que `posting_list_block_size`, ela será dividida em vários blocos, que são armazenados sequencialmente no arquivo de listas de postings.

**Arquivo de posições (.pos)**

Opcional, somente se o argumento do índice `positions = 1`.
Armazena as posições dos tokens dentro das linhas correspondentes.

**Mesclagem de índices de texto**

Quando partes de dados são mescladas, o índice de texto não precisa ser reconstruído do zero; em vez disso, ele pode ser mesclado com eficiência em uma etapa separada do processo de mesclagem.
Durante essa etapa, os dicionários ordenados dos índices de texto de cada parte de entrada são lidos e combinados em um novo dicionário unificado.
Os números das linhas nas listas de postings também são recalculados para refletir suas novas posições na parte de dados mesclada, usando um mapeamento dos números de linhas antigos para os novos, criado durante a fase inicial da mesclagem.
Esse método de mesclar índices de texto é semelhante à forma como [projeções](/pt-BR/docs/sql-reference/statements/alter/projection#projection-indexes) com a coluna `_part_offset` são mescladas.
Se o índice não estiver materializado na parte de origem, ele será criado, gravado em um arquivo temporário e depois mesclado com os índices das outras partes e de outros arquivos temporários de índice.

**Depuração**

A função de tabela [mergeTreeTextIndex](../../../sql-reference/table-functions/mergeTreeTextIndex.md) pode ser usada para inspecionar índices de texto.

<div id="hacker-news-dataset">
  ## Exemplo: conjunto de dados do Hacker News
</div>

Vamos analisar as melhorias de desempenho dos índices de texto em um grande conjunto de dados com muito conteúdo textual.
Usaremos 28,7 milhões de linhas de comentários do popular site Hacker News.
Aqui está a tabela sem índice de texto:

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

Há 28,7 milhões de linhas em um arquivo Parquet no S3 — vamos inseri-las na tabela `hackernews`:

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

Usaremos `ALTER TABLE` para adicionar um índice de texto à coluna comment e, em seguida, materializá-lo:

```sql
-- Add the index
ALTER TABLE hackernews ADD INDEX comment_idx comment TYPE text(tokenizer = splitByNonAlpha);

-- Materialize the index for existing data
ALTER TABLE hackernews MATERIALIZE INDEX comment_idx SETTINGS mutations_sync = 2;
```

Agora, vamos executar consultas usando as funções `hasToken`, `hasAnyTokens` e `hasAllTokens`.
Os exemplos a seguir mostrarão a grande diferença de desempenho entre uma varredura de índice convencional e a otimização de leitura direta.

<div id="using-hasToken">
  ### 1. Usando `hasToken`
</div>

`hasToken` verifica se o texto contém um único token específico.
Vamos buscar o token com diferenciação entre maiúsculas e minúsculas &#39;ClickHouse&#39;.

**Leitura direta desabilitada (Varredura padrão)**
Por padrão, o ClickHouse usa o skip index para filtrar grânulos e depois lê os dados da coluna desses grânulos.
Podemos simular esse comportamento desabilitando a leitura direta.

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

**Leitura direta habilitada (Fast index read)**
Agora, executamos a mesma consulta com a leitura direta habilitada (padrão).

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

A consulta com leitura direta é mais de 45 vezes mais rápida (0.362s vs 0.008s) e processa significativamente menos dados (9.51 GB vs 3.15 MB) ao ler somente o índice.

<div id="using-hasAnyTokens">
  ### 2. Usando `hasAnyTokens`
</div>

`hasAnyTokens` verifica se o texto contém pelo menos um dos tokens informados.
Vamos procurar comentários que contenham &#39;love&#39; ou &#39;ClickHouse&#39;.

**Leitura direta desativada (varredura padrão)**

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

**Leitura direta ativada (Leitura rápida do índice)**

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

O ganho de desempenho é ainda mais dramático nesta busca comum com &quot;OR&quot;.
A consulta é quase 89 vezes mais rápida (1.329s vs 0.015s) ao evitar a varredura da coluna inteira.

<div id="using-hasAllTokens">
  ### 3. Usando `hasAllTokens`
</div>

`hasAllTokens` verifica se o texto contém todos os tokens especificados.
Vamos procurar comentários que contenham tanto &#39;love&#39; quanto &#39;ClickHouse&#39;.

**Leitura direta desativada (varredura padrão)**
Mesmo com a leitura direta desativada, o skip index padrão continua sendo eficaz.
Ele reduz o conjunto de 28,7 milhões de linhas para apenas 147,46 mil linhas, mas ainda precisa ler 57,03 MB da coluna.

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

**Leitura direta ativada (Leitura rápida do índice)**
A leitura direta responde à consulta usando os dados do índice, lendo apenas 147.46 KB.

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

Para esta busca por &quot;AND&quot;, a otimização direct read é mais de 26 vezes mais rápida (0.184s vs 0.007s) do que a varredura padrão com skip index.

<div id="compound-search">
  ### 4. Busca composta: OR, AND, NOT, ...
</div>

A otimização de leitura direta também se aplica a expressões booleanas compostas.
Aqui, faremos uma busca sem diferenciar maiúsculas de minúsculas por &#39;ClickHouse&#39; OR &#39;clickhouse&#39;.

**leitura direta desativada (varredura padrão)**

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

**Leitura direta ativada (Leitura rápida do índice)**

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

Ao combinar os resultados do índice, a consulta com leitura direta fica 34 vezes mais rápida (0,450 s vs. 0,013 s) e evita a leitura de 9,58 GB de dados da coluna.
Para este caso específico, `hasAnyTokens(comment, ['ClickHouse', 'clickhouse'])` seria a sintaxe mais adequada e eficiente.

<div id="related-content">
  ## Conteúdo relacionado
</div>

* Blog: [Anunciando a Disponibilidade Geral da busca em texto completo no ClickHouse](https://clickhouse.com/blog/full-text-search-ga-release)
* Blog: [Criando busca em texto completo de alto desempenho para armazenamento de objetos](https://clickhouse.com/blog/clickhouse-full-text-search-object-storage)
* Vídeo: [Introdução à busca em texto completo no ClickHouse](https://www.youtube.com/watch?v=9zPmf1a_heU)
* Vídeo: [Por dentro: busca em texto completo no ClickHouse com escala e velocidade](https://www.youtube.com/watch?v=8JbqE_ubfkU)
* Apresentação: [Por dentro da busca em texto completo do ClickHouse: rápida, nativa e colunar](https://github.com/ClickHouse/clickhouse-presentations/blob/master/2025-tumuchdata-munich/ClickHouse_%20full-text%20search%20-%2011.11.2025%20Munich%20Database%20Meetup.pdf)
* Apresentação: [Índices invertidos de banco de dados: o porquê, o que e o como, FOSDEM 2026](https://presentations.clickhouse.com/2026-fosdem-inverted-index/Inverted_indexes_the_what_the_why_the_how.pdf)

**Material desatualizado**

* Blog: [Apresentando índices invertidos no ClickHouse](https://clickhouse.com/blog/clickhouse-search-with-inverted-indices)
* Blog: [Por dentro da busca em texto completo do ClickHouse: rápida, nativa e colunar](https://clickhouse.com/blog/clickhouse-full-text-search)
* Vídeo: [Índices de texto completo: design e experimentos](https://www.youtube.com/watch?v=O_MnyUkrIq8)