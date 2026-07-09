---
description: 'Guide de configuration de l’authentification LDAP pour ClickHouse'
slug: /operations/external-authenticators/ldap
title: 'LDAP'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

Le serveur LDAP peut être utilisé pour authentifier les utilisateurs de ClickHouse. Il existe deux approches distinctes pour cela :

* Utiliser LDAP comme authentificateur externe pour des utilisateurs existants, définis dans `users.xml` ou dans les chemins locaux du contrôle d’accès.
* Utiliser LDAP comme répertoire d’utilisateurs externe et permettre à des utilisateurs non définis localement d’être authentifiés s’ils existent sur le serveur LDAP.

Dans les deux cas, un serveur LDAP nommé en interne doit être défini dans la configuration de ClickHouse afin que les autres parties de la configuration puissent s’y référer.

<div id="ldap-server-definition">
  ## Définition du serveur LDAP
</div>

Pour définir un serveur LDAP, vous devez ajouter la section `ldap_servers` au fichier `config.xml`.

**Exemple**

```xml
<clickhouse>
    <!- ... -->
    <ldap_servers>
        <!- Typical LDAP server. -->
        <my_ldap_server>
            <host>localhost</host>
            <port>636</port>
            <bind_dn>uid={user_name},ou=users,dc=example,dc=com</bind_dn>
            <verification_cooldown>300</verification_cooldown>
            <follow_referrals>false</follow_referrals>
            <enable_tls>yes</enable_tls>
            <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>
            <tls_require_cert>demand</tls_require_cert>
            <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>
            <tls_key_file>/path/to/tls_key_file</tls_key_file>
            <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>
            <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>
            <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
        </my_ldap_server>

        <!- Typical Active Directory with configured user DN detection for further role mapping. -->
        <my_ad_server>
            <host>localhost</host>
            <port>389</port>
            <bind_dn>EXAMPLE\{user_name}</bind_dn>
            <user_dn_detection>
                <base_dn>CN=Users,DC=example,DC=com</base_dn>
                <search_filter>(&amp;(objectClass=user)(sAMAccountName={user_name}))</search_filter>
            </user_dn_detection>
            <enable_tls>no</enable_tls>
        </my_ad_server>
    </ldap_servers>
</clickhouse>
```

Notez que vous pouvez définir plusieurs serveurs LDAP dans la section `ldap_servers`, en leur attribuant des noms distincts.

**Paramètres**

| Paramètre                      | Par défaut    | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| ------------------------------ | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `host`                         | —             | Nom d’hôte ou adresse IP du serveur LDAP. Ce paramètre est obligatoire et ne peut pas être vide.                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `port`                         | `636` / `389` | Port du serveur LDAP. La valeur par défaut est `636` si `enable_tls` est défini sur `yes`, sinon `389`.                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `bind_dn`                      | —             | Modèle utilisé pour construire le DN à utiliser pour le bind. Le DN résultant est construit en remplaçant toutes les sous-chaînes `{user_name}` du modèle par le nom d’utilisateur réel à chaque tentative d’authentification.                                                                                                                                                                                                                                                                                           |
| `auth_dn_prefix`               | —             | **Obsolète.** Alternative à `bind_dn`. Ne peut pas être utilisé avec `bind_dn`. Lorsqu’il est spécifié, le bind DN est construit comme `auth_dn_prefix + {user_name} + auth_dn_suffix`. Par exemple, définir `auth_dn_prefix` sur `uid=` et `auth_dn_suffix` sur `,ou=users,dc=example,dc=com` revient à définir `bind_dn` sur `uid={user_name},ou=users,dc=example,dc=com`.                                                                                                                                             |
| `auth_dn_suffix`               | —             | **Obsolète.** Voir `auth_dn_prefix`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `verification_cooldown`        | `0`           | Durée, en secondes, après une tentative de bind réussie, pendant laquelle l’utilisateur est considéré comme authentifié avec succès pour toutes les requêtes suivantes sans contacter le serveur LDAP. Spécifiez `0` pour désactiver la mise en cache et forcer le serveur à contacter le serveur LDAP pour chaque requête d’authentification.                                                                                                                                                                           |
| `follow_referrals`             | `false`       | Indicateur permettant à la bibliothèque cliente LDAP de suivre automatiquement les referrals LDAP renvoyés par le serveur. Principalement utile dans les environnements Microsoft Active Directory, où les recherches en sous-arborescence à partir d’un base DN de niveau élevé (par ex. `DC=example,DC=com`) peuvent renvoyer des referrals/références de recherche (par ex. `DC=DomainDnsZones,...`). Définissez cette valeur sur `true` uniquement si vous avez explicitement besoin de recherches inter-partitions. |
| `enable_tls`                   | `yes`         | Indicateur déclenchant l’utilisation d’une connexion sécurisée au serveur LDAP. Spécifiez `no` pour le protocole `ldap://` en texte brut (non recommandé), `yes` pour le protocole LDAP sur SSL/TLS `ldaps://` (recommandé), ou `starttls` pour l’ancien protocole StartTLS (protocole `ldap://` en texte brut, mis à niveau vers TLS).                                                                                                                                                                                  |
| `tls_minimum_protocol_version` | `tls1.2`      | Version minimale du protocole SSL/TLS. Valeurs acceptées : `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, `tls1.2`.                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `tls_require_cert`             | `demand`      | Comportement de vérification du certificat pair SSL/TLS. Valeurs acceptées : `never`, `allow`, `try`, `demand`.                                                                                                                                                                                                                                                                                                                                                                                                          |
| `tls_cert_file`                | —             | Chemin vers le fichier de certificat.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `tls_key_file`                 | —             | Chemin vers le fichier de clé du certificat.                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `tls_ca_cert_file`             | —             | Chemin vers le fichier du certificat CA.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `tls_ca_cert_dir`              | —             | Chemin vers le répertoire contenant les certificats CA.                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `tls_cipher_suite`             | —             | Suite de chiffrement autorisée (notation OpenSSL).                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `search_limit`                 | `256`         | Nombre maximal d’entrées pouvant être renvoyées par les requêtes de recherche LDAP effectuées par cette définition de serveur (pour la détection du DN utilisateur et le mappage des rôles).                                                                                                                                                                                                                                                                                                                                  |

**Sous-paramètres de `user_dn_detection`**

Section contenant les paramètres de recherche LDAP permettant de détecter le DN utilisateur réel de l’utilisateur lié. Ils sont principalement utilisés dans les search filters pour le mappage des rôles ultérieur lorsque le serveur est Active Directory. Le DN utilisateur résultant sera utilisé lors du remplacement des sous-chaînes `{user_dn}` partout où elles sont autorisées. Par défaut, le DN utilisateur est défini comme étant égal au bind DN, mais une fois la recherche effectuée, il sera mis à jour avec la valeur réelle du DN utilisateur détecté.

| Paramètre       | Par défaut | Description                                                                                                                                                                                                                                                                                                                                                       |
| --------------- | ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base_dn`       | —          | Modèle utilisé pour construire le base DN de la recherche LDAP. Le DN résultant est construit en remplaçant toutes les sous-chaînes `{user_name}` et `{bind_dn}` du modèle par le nom d’utilisateur réel et le bind DN pendant la recherche LDAP.                                                                                                                 |
| `scope`         | `subtree`  | Portée de la recherche LDAP. Valeurs acceptées : `base`, `one_level`, `children`, `subtree`.                                                                                                                                                                                                                                                                      |
| `search_filter` | —          | Modèle utilisé pour construire le search filter de la recherche LDAP. Le filtre résultant est construit en remplaçant toutes les sous-chaînes `{user_name}`, `{bind_dn}` et `{base_dn}` du modèle par le nom d’utilisateur réel, le bind DN et le base DN pendant la recherche LDAP. Notez que les caractères spéciaux doivent être correctement échappés en XML. |

<div id="ldap-external-authenticator">
  ## Authentificateur externe LDAP
</div>

Un serveur LDAP distant peut être utilisé pour vérifier les mots de passe d’utilisateurs définis localement (utilisateurs définis dans `users.xml` ou dans les chemins locaux de contrôle d’accès). Pour cela, indiquez le nom d’un serveur LDAP précédemment défini à la place de `password` ou de sections similaires dans la définition de l’utilisateur.

À chaque tentative de connexion, ClickHouse essaie d’effectuer un &quot;bind&quot; sur le DN spécifié par le paramètre `bind_dn` dans la [définition du serveur LDAP](#ldap-server-definition) à l’aide des identifiants fournis et, en cas de succès, l’utilisateur est considéré comme authentifié. Cette méthode est souvent appelée &quot;simple bind&quot;.

**Exemple**

```xml
<clickhouse>
    <!- ... -->
    <users>
        <!- ... -->
        <my_user>
            <!- ... -->
            <ldap>
                <server>my_ldap_server</server>
            </ldap>
        </my_user>
    </users>
</clickhouse>
```

Notez que l’utilisateur `my_user` est associé à `my_ldap_server`. Ce serveur LDAP doit être configuré dans le fichier principal `config.xml`, comme décrit précédemment.

Lorsque [Contrôle d’accès et gestion des comptes](/fr/operations/access-rights#access-control-usage) piloté par SQL est activé, les utilisateurs authentifiés via des serveurs LDAP peuvent également être créés à l’aide de l’instruction [CREATE USER](/fr/sql-reference/statements/create/user).

```sql title="Query"
CREATE USER my_user IDENTIFIED WITH ldap SERVER 'my_ldap_server';
```

<div id="ldap-external-user-directory">
  ## Annuaire LDAP externe des utilisateurs
</div>

En plus des utilisateurs définis localement, un serveur LDAP distant peut être utilisé comme source de définitions d&#39;utilisateurs. Pour cela, indiquez le nom du serveur LDAP défini précédemment (voir [Définition du serveur LDAP](#ldap-server-definition)) dans la section `ldap` de la section `users_directories` du fichier `config.xml`.

À chaque tentative de connexion, ClickHouse essaie de trouver la définition de l&#39;utilisateur localement et de l&#39;authentifier comme d&#39;habitude. Si l&#39;utilisateur n&#39;est pas défini, ClickHouse supposera que sa définition existe dans l&#39;annuaire LDAP externe et tentera d&#39;effectuer un &quot;bind&quot; sur le DN spécifié sur le serveur LDAP à l&#39;aide des informations d&#39;identification fournies. En cas de réussite, l&#39;utilisateur sera considéré comme existant et authentifié. Les rôles de la liste spécifiée dans la section `roles` seront attribués à l&#39;utilisateur. En outre, une &quot;recherche&quot; LDAP peut être effectuée et ses résultats peuvent être transformés et traités comme des noms de rôles, puis attribués à l&#39;utilisateur si la section `role_mapping` est également configurée. Tout cela implique que le [Contrôle d&#39;accès et gestion des comptes](/fr/operations/access-rights#access-control-usage), piloté par SQL, est activé et que les rôles sont créés à l&#39;aide de l&#39;instruction [CREATE ROLE](/fr/sql-reference/statements/create/role).

**Exemple**

À insérer dans `config.xml`.

```xml
<clickhouse>
    <!- ... -->
    <user_directories>
        <!- Typical LDAP server. -->
        <ldap>
            <server>my_ldap_server</server>
            <roles>
                <my_local_role1 />
                <my_local_role2 />
            </roles>
            <role_mapping>
                <base_dn>ou=groups,dc=example,dc=com</base_dn>
                <scope>subtree</scope>
                <search_filter>(&amp;(objectClass=groupOfNames)(member={bind_dn}))</search_filter>
                <attribute>cn</attribute>
                <prefix>clickhouse_</prefix>
            </role_mapping>
        </ldap>

        <!- Typical Active Directory with role mapping that relies on the detected user DN. -->
        <ldap>
            <server>my_ad_server</server>
            <role_mapping>
                <base_dn>CN=Users,DC=example,DC=com</base_dn>
                <attribute>CN</attribute>
                <scope>subtree</scope>
                <search_filter>(&amp;(objectClass=group)(member={user_dn}))</search_filter>
                <prefix>clickhouse_</prefix>
            </role_mapping>
        </ldap>
    </user_directories>
</clickhouse>
```

Notez que `my_ldap_server`, mentionné dans la section `ldap` à l’intérieur de la section `user_directories`, doit être un serveur LDAP défini au préalable et configuré dans `config.xml` (voir [Définition du serveur LDAP](#ldap-server-definition)).

**Paramètres**

| Paramètre | Par défaut | Description                                                                                                                                                                                                                                                                                       |
| --------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `server`  | —          | L’un des noms de serveurs LDAP définis dans la section de configuration `ldap_servers` ci-dessus. Ce paramètre est obligatoire et ne peut pas être vide.                                                                                                                                          |
| `roles`   | —          | Section contenant une liste de rôles définis localement qui seront attribués à chaque utilisateur récupéré depuis le serveur LDAP. Si aucun rôle n’est spécifié ici ou attribué lors du mappage des rôles (ci-dessous), l’utilisateur ne pourra effectuer aucune action après l’authentification. |

**Sous-paramètres de `role_mapping`**

Section contenant les paramètres de recherche LDAP et les règles de mappage. Lorsqu’un utilisateur s’authentifie, alors que la liaison à LDAP est toujours active, une recherche LDAP est effectuée à l’aide de `search_filter` et du nom de l’utilisateur connecté. Pour chaque entrée trouvée lors de cette recherche, la valeur de l’attribut spécifié est extraite. Pour chaque valeur d’attribut qui possède le préfixe spécifié, ce préfixe est supprimé, et le reste de la valeur devient le nom d’un rôle local défini dans ClickHouse, qui doit avoir été créé au préalable à l’aide de l’instruction [CREATE ROLE](/fr/sql-reference/statements/create/role). Il peut y avoir plusieurs sections `role_mapping` définies à l’intérieur d’une même section `ldap`. Elles seront toutes appliquées.

| Paramètre       | Par défaut | Description                                                                                                                                                                                                                                                                                                                                                                                                            |
| --------------- | ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base_dn`       | —          | modèle utilisé pour construire le base DN de la recherche LDAP. Le DN résultant est construit en remplaçant toutes les sous-chaînes `{user_name}`, `{bind_dn}` et `{user_dn}` du modèle par le nom d&#39;utilisateur réel, le bind DN réel et le user DN réel lors de chaque recherche LDAP.                                                                                                                      |
| `scope`         | `subtree`  | Portée de la recherche LDAP. Valeurs acceptées : `base`, `one_level`, `children`, `subtree`.                                                                                                                                                                                                                                                                                                                           |
| `search_filter` | —          | modèle utilisé pour construire le search filter de la recherche LDAP. Le filtre résultant est construit en remplaçant toutes les sous-chaînes `{user_name}`, `{bind_dn}`, `{user_dn}` et `{base_dn}` du modèle par le nom d&#39;utilisateur réel, le bind DN réel, le user DN réel et le base DN réel lors de chaque recherche LDAP. Notez que les caractères spéciaux doivent être correctement échappés en XML. |
| `attribute`     | `cn`       | Nom de l&#39;attribut dont les valeurs sont renvoyées par la recherche LDAP.                                                                                                                                                                                                                                                                                                                                           |
| `prefix`        | vide       | Préfixe attendu au début de chaque chaîne dans la liste d&#39;origine des chaînes renvoyées par la recherche LDAP. Le préfixe est supprimé des chaînes d&#39;origine, et les chaînes obtenues sont traitées comme des noms de rôle locaux.                                                                                                                                                                             |