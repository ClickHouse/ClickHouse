---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 50
toc_title: Fichiers De Configuration
---

# Fichiers De Configuration {#configuration_files}

ClickHouse prend en charge la gestion de la configuration multi-fichiers. Le fichier de configuration du serveur principal est `/etc/clickhouse-server/config.xml`. Les autres fichiers doivent être dans le `/etc/clickhouse-server/config.d` répertoire.

!!! note "Note"
    Tous les fichiers de configuration doivent être au format XML. Aussi, ils doivent avoir le même élément racine, généralement `<yandex>`.

Certains paramètres spécifiés dans le fichier de configuration principal peuvent être remplacés dans d'autres fichiers de configuration. Le `replace` ou `remove` les attributs peuvent être spécifiés pour les éléments de ces fichiers de configuration.

Si ni l'un ni l'autre n'est spécifié, il combine le contenu des éléments de manière récursive, remplaçant les valeurs des enfants en double.

Si `replace` est spécifié, il remplace l'élément entier par celui spécifié.

Si `remove` est spécifié, il supprime l'élément.

La configuration peut également définir “substitutions”. Si un élément a le `incl` attribut, la substitution correspondante du fichier sera utilisée comme valeur. Par défaut, le chemin d'accès au fichier avec des substitutions est `/etc/metrika.xml`. Ceci peut être changé dans le [include\_from](server-configuration-parameters/settings.md#server_configuration_parameters-include_from) élément dans la configuration du serveur. Les valeurs de substitution sont spécifiées dans `/yandex/substitution_name` les éléments de ce fichier. Si une substitution spécifiée dans `incl` n'existe pas, il est enregistré dans le journal. Pour empêcher ClickHouse de consigner les substitutions manquantes, spécifiez `optional="true"` attribut (par exemple, les paramètres de [macro](server-configuration-parameters/settings.md)).

Les Substitutions peuvent également être effectuées à partir de ZooKeeper. Pour ce faire, spécifiez l'attribut `from_zk = "/path/to/node"`. La valeur de l'élément est remplacé par le contenu du noeud au `/path/to/node` dans ZooKeeper. Vous pouvez également placer un sous-arbre XML entier sur le nœud ZooKeeper et il sera entièrement inséré dans l'élément source.

Le `config.xml` le fichier peut spécifier une configuration distincte avec les paramètres utilisateur, les profils et les quotas. Le chemin relatif à cette configuration est défini dans `users_config` élément. Par défaut, il est `users.xml`. Si `users_config` est omis, les paramètres utilisateur, les profils et les quotas sont `config.xml`.

La configuration des utilisateurs peut être divisée en fichiers séparés similaires à `config.xml` et `config.d/`.
Nom du répertoire est défini comme `users_config` sans `.xml` postfix concaténé avec `.d`.
Répertoire `users.d` est utilisé par défaut, comme `users_config` par défaut `users.xml`.
Par exemple, vous pouvez avoir séparé fichier de configuration pour chaque utilisateur comme ceci:

``` bash
$ cat /etc/clickhouse-server/users.d/alice.xml
```

``` xml
<yandex>
    <users>
      <alice>
          <profile>analytics</profile>
            <networks>
                  <ip>::/0</ip>
            </networks>
          <password_sha256_hex>...</password_sha256_hex>
          <quota>analytics</quota>
      </alice>
    </users>
</yandex>
```

Pour chaque fichier de configuration, le serveur génère également `file-preprocessed.xml` les fichiers lors du démarrage. Ces fichiers contiennent toutes les remplacements et des remplacements, et ils sont destinés à l'usage informatif. Si des substitutions ZooKeeper ont été utilisées dans les fichiers de configuration mais que ZooKeeper n'est pas disponible au démarrage du serveur, le serveur charge la configuration à partir du fichier prétraité.

Le serveur suit les changements dans les fichiers de configuration, ainsi que les fichiers et les nœuds ZooKeeper utilisés lors des substitutions et des remplacements, et recharge les paramètres pour les utilisateurs et les clusters à la volée. Cela signifie que vous pouvez modifier le cluster, les utilisateurs et leurs paramètres sans redémarrer le serveur.

[Article Original](https://clickhouse.tech/docs/en/operations/configuration_files/) <!--hide-->
