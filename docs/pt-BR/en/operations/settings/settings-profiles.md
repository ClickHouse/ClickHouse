---
description: 'Uma coleção de configurações agrupadas sob o mesmo nome.'
sidebar_label: 'Perfis de configurações'
sidebar_position: 61
slug: /operations/settings/settings-profiles
title: 'Perfis de configurações'
doc_type: 'referência'
---

Um perfil de configurações é uma coleção de configurações agrupadas sob o mesmo nome.

:::note
O ClickHouse também oferece suporte a [fluxo de trabalho orientado por SQL](/pt-BR/operations/access-rights#access-control-usage) para gerenciar perfis de configurações. Recomendamos usá-lo.
:::

O perfil pode ter qualquer nome. Você pode especificar o mesmo perfil para diferentes usuários. A configuração mais importante que você pode definir no perfil de configurações é `readonly=1`, que garante acesso somente leitura.

Perfis de configurações podem herdar uns dos outros. Para usar herança, indique uma ou mais configurações `profile` antes das outras configurações listadas no perfil. Caso uma configuração seja definida em perfis diferentes, prevalecerá a definida por último.

Para aplicar todas as configurações de um perfil, defina a configuração `profile`.

Exemplo:

Instale o perfil `web`.

```sql
SET profile = 'web'
```

Os perfis de configuração são declarados no arquivo de configuração de usuários. Normalmente, é `users.xml`.

Exemplo:

```xml
<!-- Settings profiles -->
<profiles>
    <!-- Default settings -->
    <default>
        <!-- The maximum number of threads when running a single query. -->
        <max_threads>8</max_threads>
    </default>

    <!-- Background operations settings -->
    <background>
        <!-- Re-defining maximum number of threads for background operations -->
        <max_threads>12</max_threads>
    </background>

    <!-- Settings for queries from the user interface -->
    <web>
        <max_rows_to_read>1000000000</max_rows_to_read>
        <max_bytes_to_read>100000000000</max_bytes_to_read>

        <max_rows_to_group_by>1000000</max_rows_to_group_by>
        <group_by_overflow_mode>any</group_by_overflow_mode>

        <max_rows_to_sort>1000000</max_rows_to_sort>
        <max_bytes_to_sort>1000000000</max_bytes_to_sort>

        <max_result_rows>100000</max_result_rows>
        <max_result_bytes>100000000</max_result_bytes>
        <result_overflow_mode>break</result_overflow_mode>

        <max_execution_time>600</max_execution_time>
        <min_execution_speed>1000000</min_execution_speed>
        <timeout_before_checking_execution_speed>15</timeout_before_checking_execution_speed>

        <max_columns_to_read>25</max_columns_to_read>
        <max_temporary_columns>100</max_temporary_columns>
        <max_temporary_non_const_columns>50</max_temporary_non_const_columns>

        <max_subquery_depth>2</max_subquery_depth>
        <max_pipeline_depth>25</max_pipeline_depth>
        <max_ast_depth>50</max_ast_depth>
        <max_ast_elements>100</max_ast_elements>

        <max_sessions_for_user>4</max_sessions_for_user>

        <readonly>1</readonly>
    </web>
</profiles>
```

O exemplo especifica dois perfis: `default` e `web`.

O perfil `default` tem uma finalidade especial: ele deve estar sempre presente e é aplicado na inicialização do servidor. Em outras palavras, o perfil `default` contém as configurações padrão. O nome do perfil padrão pode ser alterado por meio da configuração `default_profile` do servidor.

O perfil `background` tem uma finalidade especial: ele pode estar presente para sobrescrever configurações de operações em segundo plano. O parâmetro é opcional, e seu nome pode ser alterado por meio da configuração `background_profile` do servidor.

O perfil `web` é um perfil comum que pode ser definido usando a consulta `SET` ou um parâmetro de URL em uma consulta HTTP.