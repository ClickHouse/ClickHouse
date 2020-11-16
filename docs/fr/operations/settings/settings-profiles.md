---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: "Les Param\xE8tres Des Profils"
---

# Les Paramètres Des Profils {#settings-profiles}

Un profil de paramètres est une collection de paramètres regroupés sous le même nom.

!!! note "Information"
    Clickhouse prend également en charge [Flux de travail piloté par SQL](../access-rights.md#access-control) pour gérer les profils de paramètres. Nous vous conseillons de l'utiliser.

Un profil peut avoir n'importe quel nom. Le profil peut avoir n'importe quel nom. Vous pouvez spécifier le même profil pour différents utilisateurs. La chose la plus importante que vous pouvez écrire dans les paramètres de profil `readonly=1` qui assure un accès en lecture seule.

Paramètres les profils peuvent hériter les uns des autres. Pour utiliser l'héritage, indiquer un ou plusieurs `profile` paramètres avant les autres paramètres répertoriés dans le profil. Dans le cas où un paramètre est défini dans les différents profils, les dernières définie est utilisée.

Pour appliquer tous les paramètres d'un profil, définissez `profile` paramètre.

Exemple:

Installer le `web` profil.

``` sql
SET profile = 'web'
```

Les profils de paramètres sont déclarés dans le fichier de configuration utilisateur. Ce n'est généralement `users.xml`.

Exemple:

``` xml
<!-- Settings profiles -->
<profiles>
    <!-- Default settings -->
    <default>
        <!-- The maximum number of threads when running a single query. -->
        <max_threads>8</max_threads>
    </default>

    <!-- Settings for quries from the user interface -->
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

        <readonly>1</readonly>
    </web>
</profiles>
```

L'exemple spécifie deux profils: `default` et `web`.

Le `default` profil a un but particulier: il doit toujours être présent et est appliquée lors du démarrage du serveur. En d'autres termes, l' `default` profil contient les paramètres par défaut.

Le `web` profil est un profil régulier qui peut être défini à l'aide `SET` requête ou en utilisant un paramètre URL dans une requête HTTP.

[Article Original](https://clickhouse.tech/docs/en/operations/settings/settings_profiles/) <!--hide-->
