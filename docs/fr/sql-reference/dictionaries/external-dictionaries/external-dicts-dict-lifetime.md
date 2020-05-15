---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 42
toc_title: "Mises \xC0 Jour Du Dictionnaire"
---

# Mises À Jour Du Dictionnaire {#dictionary-updates}

ClickHouse met périodiquement à jour les dictionnaires. L'intervalle de mise à jour pour les dictionnaires entièrement téléchargés et l'intervalle d'invalidation pour les dictionnaires `<lifetime>` tag en quelques secondes.

Les mises à jour du dictionnaire (autres que le chargement pour la première utilisation) ne bloquent pas les requêtes. Lors des mises à jour, l'ancienne version d'un dictionnaire est utilisée. Si une erreur se produit pendant une mise à jour, l'erreur est écrite dans le journal du serveur et les requêtes continuent d'utiliser l'ancienne version des dictionnaires.

Exemple de paramètres:

``` xml
<dictionary>
    ...
    <lifetime>300</lifetime>
    ...
</dictionary>
```

``` sql
CREATE DICTIONARY (...)
...
LIFETIME(300)
...
```

Paramètre `<lifetime>0</lifetime>` (`LIFETIME(0)`) empêche la mise à jour des dictionnaires.

Vous pouvez définir un intervalle de temps pour les mises à niveau, et ClickHouse choisira un temps uniformément aléatoire dans cette plage. Ceci est nécessaire pour répartir la charge sur la source du dictionnaire lors de la mise à niveau sur un grand nombre de serveurs.

Exemple de paramètres:

``` xml
<dictionary>
    ...
    <lifetime>
        <min>300</min>
        <max>360</max>
    </lifetime>
    ...
</dictionary>
```

ou

``` sql
LIFETIME(MIN 300 MAX 360)
```

Si `<min>0</min>` et `<max>0</max>`, ClickHouse ne recharge pas le dictionnaire par timeout.
Dans ce cas, ClickHouse peut recharger le dictionnaire plus tôt si le fichier de configuration du dictionnaire a été `SYSTEM RELOAD DICTIONARY` la commande a été exécutée.

Lors de la mise à niveau des dictionnaires, le serveur ClickHouse applique une logique différente selon le type de [source](external-dicts-dict-sources.md):

Lors de la mise à niveau des dictionnaires, le serveur ClickHouse applique une logique différente selon le type de [source](external-dicts-dict-sources.md):

-   Pour un fichier texte, il vérifie l'heure de la modification. Si l'heure diffère de l'heure enregistrée précédemment, le dictionnaire est mis à jour.
-   Pour les tables MyISAM, l'Heure de modification est vérifiée à l'aide d'un `SHOW TABLE STATUS` requête.
-   Les dictionnaires d'autres sources sont mis à jour à chaque fois par défaut.

Pour les sources MySQL (InnoDB), ODBC et ClickHouse, vous pouvez configurer une requête qui mettra à jour les dictionnaires uniquement s'ils ont vraiment changé, plutôt que chaque fois. Pour ce faire, suivez ces étapes:

-   La table de dictionnaire doit avoir un champ qui change toujours lorsque les données source sont mises à jour.
-   Les paramètres de la source doivent spécifier une requête qui récupère le champ de modification. Le serveur ClickHouse interprète le résultat de la requête comme une ligne, et si cette ligne a changé par rapport à son état précédent, le dictionnaire est mis à jour. Spécifier la requête dans le `<invalidate_query>` champ dans les paramètres pour le [source](external-dicts-dict-sources.md).

Exemple de paramètres:

``` xml
<dictionary>
    ...
    <odbc>
      ...
      <invalidate_query>SELECT update_time FROM dictionary_source where id = 1</invalidate_query>
    </odbc>
    ...
</dictionary>
```

ou

``` sql
...
SOURCE(ODBC(... invalidate_query 'SELECT update_time FROM dictionary_source where id = 1'))
...
```

[Article Original](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_lifetime/) <!--hide-->
