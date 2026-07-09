---
description: 'Documentação do tipo de dado Enum no ClickHouse, que representa
  um conjunto de valores constantes nomeados'
sidebar_label: 'Enum'
sidebar_position: 20
slug: /sql-reference/data-types/enum
title: 'Enum'
doc_type: 'reference'
---

Tipo enumerado composto por valores nomeados.

Os valores nomeados podem ser declarados como pares de `'string' = integer` ou como nomes `'string'`. O ClickHouse armazena apenas números, mas oferece suporte a operações com esses valores por meio de seus nomes.

O ClickHouse oferece suporte a:

* `Enum` de 8 bits. Ele pode conter até 256 valores enumerados no intervalo `[-128, 127]`.
* `Enum` de 16 bits. Ele pode conter até 65536 valores enumerados no intervalo `[-32768, 32767]`.

O ClickHouse escolhe automaticamente o tipo de `Enum` quando os dados são inseridos. Você também pode usar os tipos `Enum8` ou `Enum16` para garantir o tamanho do armazenamento.

<div id="usage-examples">
  ## Exemplos de uso
</div>

Aqui, criamos uma tabela com uma coluna do tipo `Enum8('hello' = 1, 'world' = 2)`:

```sql
CREATE TABLE t_enum
(
    x Enum('hello' = 1, 'world' = 2)
)
ENGINE = TinyLog
```

Da mesma forma, você pode omitir os números. O ClickHouse atribuirá números consecutivos automaticamente. Por padrão, os números são atribuídos a partir de 1.

```sql
CREATE TABLE t_enum
(
    x Enum('hello', 'world')
)
ENGINE = TinyLog
```

Você também pode especificar um número inicial permitido para o primeiro nome.

```sql
CREATE TABLE t_enum
(
    x Enum('hello' = 1, 'world')
)
ENGINE = TinyLog
```

```sql
CREATE TABLE t_enum
(
    x Enum8('hello' = -129, 'world')
)
ENGINE = TinyLog
```

```text
Exception on server:
Code: 69. DB::Exception: Value -129 for element 'hello' exceeds range of Enum8.
```

A coluna `x` só pode armazenar valores listados na definição do tipo: `'hello'` ou `'world'`. Se você tentar salvar qualquer outro valor, ClickHouse gerará uma exceção. O tamanho de 8 bits deste `Enum` é escolhido automaticamente.

```sql
INSERT INTO t_enum VALUES ('hello'), ('world'), ('hello')
```

```text
Ok.
```

```sql
INSERT INTO t_enum VALUES('a')
```

```text
Exception on client:
Code: 49. DB::Exception: Unknown element 'a' for type Enum('hello' = 1, 'world' = 2)
```

Quando você consulta dados da tabela, o ClickHouse retorna os valores textuais de `Enum`.

```sql
SELECT * FROM t_enum
```

```text
┌─x─────┐
│ hello │
│ world │
│ hello │
└───────┘
```

Se precisar ver os equivalentes numéricos das linhas, será necessário converter o valor `Enum` para um tipo inteiro.

```sql
SELECT CAST(x, 'Int8') FROM t_enum
```

```text
┌─CAST(x, 'Int8')─┐
│               1 │
│               2 │
│               1 │
└─────────────────┘
```

Para criar um valor de Enum em uma consulta, você também precisa usar `CAST`.

```sql
SELECT toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))
```

```text
┌─toTypeName(CAST('a', 'Enum(\'a\' = 1, \'b\' = 2)'))─┐
│ Enum8('a' = 1, 'b' = 2)                             │
└─────────────────────────────────────────────────────┘
```

<div id="general-rules-and-usage">
  ## Regras gerais e uso
</div>

Cada um dos valores recebe um número no intervalo `-128 ... 127` para `Enum8` ou no intervalo `-32768 ... 32767` para `Enum16`. Todas as strings e números devem ser diferentes. Uma string vazia é permitida. Se esse tipo for especificado (em uma definição de tabela), os números podem estar em qualquer ordem. No entanto, a ordem não importa.

Nem a string nem o valor numérico em um `Enum` podem ser [NULL](../../sql-reference/syntax.md).

Um `Enum` pode estar contido no tipo [Nullable](../../sql-reference/data-types/nullable.md). Portanto, se você criar uma tabela usando a consulta

```sql
CREATE TABLE t_enum_nullable
(
    x Nullable( Enum8('hello' = 1, 'world' = 2) )
)
ENGINE = TinyLog
```

ele pode armazenar não apenas `'hello'` e `'world'`, mas também `NULL`.

```sql
INSERT INTO t_enum_nullable VALUES('hello'),('world'),(NULL)
```

Na RAM, uma coluna `Enum` é armazenada da mesma forma que `Int8` ou `Int16` com os valores numéricos correspondentes.

Ao ler em formato de texto, o ClickHouse analisa o valor como uma string e procura a string correspondente no conjunto de valores do Enum. Se ela não for encontrada, uma exceção é lançada. Ao ler em formato de texto, a string é lida e o valor numérico correspondente é buscado. Uma exceção será lançada se ele não for encontrado.
Ao gravar em formato de texto, o valor é gravado como a string correspondente. Se os dados da coluna contiverem lixo (números que não pertencem ao conjunto válido), uma exceção é lançada. Ao ler e gravar em forma binária, funciona da mesma forma que para os tipos de dados Int8 e Int16.
O valor padrão implícito é o valor com o menor número.

Durante `ORDER BY`, `GROUP BY`, `IN`, `DISTINCT` e assim por diante, Enums se comportam da mesma forma que os números correspondentes. Por exemplo, ORDER BY os ordena numericamente. Os operadores de igualdade e comparação funcionam da mesma forma em Enums e nos valores numéricos subjacentes.

Valores de Enum não podem ser comparados com números. Enums podem ser comparados com uma string constante. Se a string usada na comparação não for um valor válido para o Enum, uma exceção será lançada. O operador IN é compatível com o Enum no lado esquerdo e um conjunto de strings no lado direito. As strings são os valores do Enum correspondente.

A maioria das operações numéricas e de string não está definida para valores Enum, por exemplo, adicionar um número a um Enum ou concatenar uma string a um Enum.
No entanto, o Enum tem uma função `toString` nativa que retorna seu valor em string.

Os valores Enum também podem ser convertidos em tipos numéricos usando a função `toT`, em que T é um tipo numérico. Quando T corresponde ao tipo numérico subjacente do enum, essa conversão tem custo zero.
O tipo Enum pode ser alterado sem custo usando ALTER, se apenas o conjunto de valores for alterado. É possível adicionar e remover membros do Enum usando ALTER (a remoção só é segura se o valor removido nunca tiver sido usado na tabela). Como medida de proteção, alterar o valor numérico de um membro do Enum definido anteriormente lançará uma exceção.

Usando ALTER, é possível alterar um Enum8 para um Enum16 ou vice-versa, assim como ao alterar um Int8 para Int16.

<div id="add-enum-values">
  ## ADICIONAR VALORES AO ENUM
</div>

Há um açúcar sintático para adicionar novos valores a um enum usando ALTER [MODIFY COLUMN ADD ENUM VALUES](../../sql-reference/statements/alter/column.md#modify-column-add-enum-values)

```sql
CREATE TABLE enum
(
    x Enum('One' = 1, 'Two', 'Three')
) ENGINE = Memory;
ALTER TABLE enum MODIFY COLUMN x ADD ENUM VALUES ('Zero' = 0, 'Four' = 4);
SHOW CREATE TABLE enum;
```

```text
┌─statement────────────────────────────────────────────────────────────────┐
│CREATE TABLE default.enum                                                 │
│(                                                                         │
│    `x` Enum8('Zero' = 0, 'One' = 1, 'Two' = 2, 'Three' = 3, 'Four' = 4)  │
│)                                                                         │
│ENGINE = Memory                                                           │
└──────────────────────────────────────────────────────────────────────────┘
```