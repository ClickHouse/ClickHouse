---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 62
toc_title: "Contraintes sur les param\xE8tres"
---

# Contraintes sur les paramètres {#constraints-on-settings}

Les contraintes sur les paramètres peuvent être définis dans le `profiles` la section de la `user.xml` fichier de configuration et interdire aux utilisateurs de modifier certains `SET` requête.
Les contraintes sont définies comme suit:

``` xml
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
    </constraints>
  </user_name>
</profiles>
```

Si l'utilisateur tente de violer les contraintes une exception est levée et le réglage n'est pas modifié.
Trois types de contraintes sont pris en charge: `min`, `max`, `readonly`. Le `min` et `max` les contraintes spécifient les limites supérieure et inférieure pour un paramètre numérique et peuvent être utilisées en combinaison. Le `readonly` contrainte spécifie que l'utilisateur ne peut pas modifier le paramètre correspondant à tous.

**Exemple:** Laisser `users.xml` comprend des lignes:

``` xml
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

Les requêtes suivantes toutes les exceptions throw:

``` sql
SET max_memory_usage=20000000001;
SET max_memory_usage=4999999999;
SET force_index_by_date=1;
```

``` text
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be greater than 20000000000.
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be less than 5000000000.
Code: 452, e.displayText() = DB::Exception: Setting force_index_by_date should not be changed.
```

**Note:** le `default` le profil a une manipulation particulière: toutes les contraintes définies pour `default` le profil devient les contraintes par défaut, de sorte qu'ils restreignent tous les utilisateurs jusqu'à ce qu'ils soient explicitement remplacés pour ces utilisateurs.

[Article Original](https://clickhouse.tech/docs/en/operations/settings/constraints_on_settings/) <!--hide-->
