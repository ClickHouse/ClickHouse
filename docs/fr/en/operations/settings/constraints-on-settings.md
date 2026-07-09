---
description: 'Des contraintes sur les paramètres peuvent être définies dans la section `profiles`
  du fichier de configuration `user.xml` et interdisent aux utilisateurs de modifier
  certains paramètres au moyen de la requête `SET`.'
sidebar_label: 'Contraintes sur les paramètres'
sidebar_position: 62
slug: /operations/settings/constraints-on-settings
title: 'Contraintes sur les paramètres'
doc_type: 'reference'
---

<div id="overview">
  ## Vue d’ensemble
</div>

Dans ClickHouse, les « contraintes » appliquées aux paramètres correspondent à des limitations et à des règles
que vous pouvez définir pour ces paramètres. Ces contraintes peuvent être utilisées pour garantir
la stabilité, la sécurité et le comportement prévisible de votre base de données.

<div id="defining-constraints">
  ## Définir des contraintes
</div>

Les contraintes sur les paramètres peuvent être définies dans la section `profiles` du
fichier de configuration `user.xml`. Elles empêchent les utilisateurs de modifier certains paramètres à l’aide de l’
[instruction `SET`](/fr/sql-reference/statements/set).

Les contraintes sont définies comme suit :

```xml
<profiles>
  <user_name>
    <constraints>
      <setting_name_1>
        <min>lower_boundary</min>
      </setting_name_1>
      <setting_name_2>
        <max>upper_boundary</max>
      </setting_name_2>
      <setting_name_3>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
      </setting_name_3>
      <setting_name_4>
        <readonly/>
      </setting_name_4>
      <setting_name_5>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
        <changeable_in_readonly/>
      </setting_name_5>
      <setting_name_6>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
        <disallowed>value1</disallowed>
        <disallowed>value2</disallowed>
        <disallowed>value3</disallowed>
        <changeable_in_readonly/>
      </setting_name_6>
    </constraints>
  </user_name>
</profiles>
```

Si l’utilisateur essaie d’enfreindre les contraintes, une exception est levée et le
paramètre reste inchangé.

<div id="types-of-constraints">
  ## Types de contraintes
</div>

ClickHouse prend en charge plusieurs types de contraintes :

* `min`
* `max`
* `disallowed`
* `readonly` (avec l’alias `const`)
* `changeable_in_readonly`

Les contraintes `min` et `max` définissent les limites inférieure et supérieure d’un
paramètre numérique et peuvent être utilisées conjointement.

La contrainte `disallowed` peut être utilisée pour spécifier une ou plusieurs valeurs précises qui ne
doivent pas être autorisées pour un paramètre donné.

La contrainte `readonly` ou `const` indique que l’utilisateur ne peut pas modifier
le paramètre correspondant.

Le type de contrainte `changeable_in_readonly` permet aux utilisateurs de modifier le paramètre
dans la plage `min`/`max` même si le paramètre `readonly` est défini sur `1` ;
sinon, les paramètres ne peuvent pas être modifiés en mode `readonly=1`.

:::note
`changeable_in_readonly` n’est pris en charge que si `settings_constraints_replace_previous`
est activé :

```xml
<access_control_improvements>
  <settings_constraints_replace_previous>true</settings_constraints_replace_previous>
</access_control_improvements>
```

:::

<div id="multiple-constraint-profiles">
  ## Plusieurs profils de contraintes
</div>

S’il y a plusieurs profils actifs pour un utilisateur, les contraintes sont fusionnées.
Le processus de fusion dépend de `settings_constraints_replace_previous` :

* **true** (recommandé) : les contraintes portant sur le même paramètre sont remplacées lors de la
  fusion, de sorte que la dernière contrainte est utilisée et que toutes les précédentes sont ignorées.
  Cela inclut les champs qui ne sont pas définis dans la nouvelle contrainte.
* **false** (par défaut) : les contraintes portant sur le même paramètre sont fusionnées de telle sorte que
  chaque type de contrainte non défini est repris du profil précédent, et que chaque
  type de contrainte défini est remplacé par la valeur du nouveau profil.

<div id="read-only">
  ## Mode en lecture seule
</div>

Le mode en lecture seule est activé par le paramètre `readonly`, à ne pas confondre
avec le type de contrainte `readonly` :

* `readonly=0` : Aucune restriction de lecture seule.
* `readonly=1` : Seules les requêtes de lecture sont autorisées et les paramètres ne peuvent pas être modifiés
  sauf si `changeable_in_readonly` est défini.
* `readonly=2` : Seules les requêtes de lecture sont autorisées, mais les paramètres peuvent être modifiés,
  à l&#39;exception du paramètre `readonly` lui-même.

<div id="example-read-only">
  ### Exemple
</div>

Ajoutez les lignes suivantes à `users.xml` :

```xml
<profiles>
  <default>
    <max_memory_usage>10000000000</max_memory_usage>
    <force_index_by_date>0</force_index_by_date>
    ...
    <constraints>
      <max_memory_usage>
        <min>5000000000</min>
        <max>20000000000</max>
      </max_memory_usage>
      <force_index_by_date>
        <readonly/>
      </force_index_by_date>
    </constraints>
  </default>
</profiles>
```

Les requêtes suivantes généreront toutes une exception :

```sql
SET max_memory_usage=20000000001;
SET max_memory_usage=4999999999;
SET force_index_by_date=1;
```

```text
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be greater than 20000000000.
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be less than 5000000000.
Code: 452, e.displayText() = DB::Exception: Setting force_index_by_date should not be changed.
```

:::note
Le profil `default` est traité de manière particulière : toutes les contraintes définies pour le
profil `default` deviennent les contraintes par défaut et s’appliquent donc à tous les utilisateurs
jusqu’à ce qu’elles soient explicitement remplacées pour ces utilisateurs.
:::

<div id="constraints-on-merge-tree-settings">
  ## Contraintes sur les paramètres MergeTree
</div>

Il est possible de définir des contraintes pour les [paramètres MergeTree](merge-tree-settings.md).
Ces contraintes sont appliquées lors de la création d&#39;une table avec le moteur MergeTree
ou de la modification de ses paramètres de stockage.

Le nom d&#39;un paramètre MergeTree doit être précédé du préfixe `merge_tree_` lorsqu&#39;il est
référencé dans la section `<constraints>`.

<div id="example-read-only">
  ### Exemple
</div>

Vous pouvez interdire la création de nouvelles tables lorsque `storage_policy` est spécifié explicitement

```xml
<profiles>
  <default>
    <constraints>
      <merge_tree_storage_policy>
        <const/>
      </merge_tree_storage_policy>
    </constraints>
  </default>
</profiles>
```