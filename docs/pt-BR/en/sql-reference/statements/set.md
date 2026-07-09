---
description: 'Documentação da instrução SET'
sidebar_label: 'SET'
sidebar_position: 50
slug: /sql-reference/statements/set
title: 'Instrução SET'
doc_type: 'referência'
---

```sql
SET param = value
```

Atribui `value` à [configuração](/pt-BR/operations/settings/overview) `param` da sessão atual. Você não pode alterar as [configurações do servidor](../../operations/server-configuration-parameters/settings.md) dessa forma.

Você também pode definir, em uma única consulta, todos os valores do perfil de configurações especificado.

```sql
SET profile = 'profile-name-from-the-settings-file'
```

Para configurações booleanas definidas como true, você pode usar uma sintaxe abreviada, omitindo a atribuição do valor. Quando apenas o nome da configuração é especificado, ela é definida automaticamente como `1` (true).

```sql
-- These are equivalent:
SET force_index_by_date = 1
SET force_index_by_date
```

<div id="set-time-zone">
  ## SET TIME ZONE
</div>

```sql
SET TIME ZONE [=] 'timezone'
```

Define o fuso horário da sessão. Este é um alias para `SET session_timezone = 'timezone'`, disponibilizado para compatibilidade com o PostgreSQL e outros bancos de dados SQL.

Muitos clientes SQL, ORMs e drivers JDBC executam automaticamente `SET TIME ZONE` ao se conectar. Essa sintaxe permite que essas ferramentas funcionem com o ClickHouse sem a necessidade de soluções alternativas personalizadas.

```sql
SET TIME ZONE 'UTC';
SET TIME ZONE 'Europe/Amsterdam';
SET TIME ZONE 'America/New_York';

-- Verify the current session time zone
SELECT getSetting('session_timezone');
```

O valor de timezone deve ser um nome válido do [IANA Time Zone Database](https://www.iana.org/time-zones). Um nome de timezone inválido causará um erro.

Para mais informações sobre a configuração `session_timezone`, consulte [session&#95;timezone](/pt-BR/operations/settings/settings#session_timezone).

<div id="setting-query-parameters">
  ## Definindo parâmetros de consulta
</div>

A instrução `SET` também pode ser usada para definir parâmetros de consulta, prefixando o nome do parâmetro com `param_`.
Os parâmetros de consulta permitem escrever consultas genéricas com placeholders que são substituídos por valores reais em tempo de execução.

```sql
SET param_name = value
```

Para usar um parâmetro de consulta em sua consulta, faça referência a ele usando a sintaxe `{name: datatype}`:

```sql
SET param_id = 42;
SET param_name = 'John';

SELECT * FROM users
WHERE id = {id: UInt32}
AND name = {name: String};
```

Os parâmetros de consulta são particularmente úteis quando a mesma consulta precisa ser executada várias vezes com valores diferentes.

Para informações mais detalhadas sobre parâmetros de consulta, incluindo o uso com o tipo `Identifier`, consulte [Definindo e usando parâmetros de consulta](../../sql-reference/syntax.md#defining-and-using-query-parameters).

Para mais informações, consulte [Configuração](../../operations/settings/settings.md).