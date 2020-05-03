---
machine_translated: true
machine_translated_rev: f865c9653f9df092694258e0ccdd733c339112f5
toc_priority: 48
toc_title: "Les Droits D'Acc\xE8s"
---

# Les Droits D’Accès {#access-rights}

Les utilisateurs et les droits d’accès sont configurés dans la configuration utilisateur. Ce n’est généralement `users.xml`.

Les utilisateurs sont enregistrés dans le `users` section. Voici un fragment de la `users.xml` fichier:

``` xml
<!-- Users and ACL. -->
<users>
    <!-- If the user name is not specified, the 'default' user is used. -->
    <default>
        <!-- Password could be specified in plaintext or in SHA256 (in hex format).

             If you want to specify password in plaintext (not recommended), place it in 'password' element.
             Example: <password>qwerty</password>.
             Password could be empty.

             If you want to specify SHA256, place it in 'password_sha256_hex' element.
             Example: <password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>

             How to generate decent password:
             Execute: PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'
             In first line will be password and in second - corresponding SHA256.
        -->
        <password></password>

        <!-- A list of networks that access is allowed from.
            Each list item has one of the following forms:
            <ip> The IP address or subnet mask. For example: 198.51.100.0/24 or 2001:DB8::/32.
            <host> Host name. For example: example01. A DNS query is made for verification, and all addresses obtained are compared with the address of the customer.
            <host_regexp> Regular expression for host names. For example, ^example\d\d-\d\d-\d\.host\.ru$
                To check it, a DNS PTR request is made for the client's address and a regular expression is applied to the result.
                Then another DNS query is made for the result of the PTR query, and all received address are compared to the client address.
                We strongly recommend that the regex ends with \.host\.ru$.

            If you are installing ClickHouse yourself, specify here:
                <networks>
                        <ip>::/0</ip>
                </networks>
        -->
        <networks incl="networks" />

        <!-- Settings profile for the user. -->
        <profile>default</profile>

        <!-- Quota for the user. -->
        <quota>default</quota>
    </default>

    <!-- For requests from the Yandex.Metrica user interface via the API for data on specific counters. -->
    <web>
        <password></password>
        <networks incl="networks" />
        <profile>web</profile>
        <quota>default</quota>
        <allow_databases>
           <database>test</database>
        </allow_databases>
        <allow_dictionaries>
           <dictionary>test</dictionary>
        </allow_dictionaries>
    </web>
</users>
```

Vous pouvez voir une déclaration de deux utilisateurs: `default`et`web`. Nous avons ajouté l’ `web` utilisateur séparément.

Le `default` l’utilisateur est choisi dans les cas où le nom d’utilisateur n’est pas passé. Le `default` l’utilisateur est également utilisé pour le traitement des requêtes distribuées, si la configuration du serveur ou du cluster `user` et `password` (voir la section sur les [Distribué](../engines/table-engines/special/distributed.md) moteur).

The user that is used for exchanging information between servers combined in a cluster must not have substantial restrictions or quotas – otherwise, distributed queries will fail.

Le mot de passe est spécifié en texte clair (non recommandé) ou en SHA-256. Le hash n’est pas salé. À cet égard, vous ne devez pas considérer ces mots de passe comme assurant la sécurité contre les attaques malveillantes potentielles. Au contraire, ils sont nécessaires pour la protection contre les employés.

Une liste de réseaux est précisé que l’accès est autorisé à partir. Dans cet exemple, la liste des réseaux pour les utilisateurs est chargé à partir d’un fichier séparé (`/etc/metrika.xml`) contenant les `networks` substitution. Voici un fragment de:

``` xml
<yandex>
    ...
    <networks>
        <ip>::/64</ip>
        <ip>203.0.113.0/24</ip>
        <ip>2001:DB8::/32</ip>
        ...
    </networks>
</yandex>
```

Vous pouvez définir cette liste de réseaux directement dans `users.xml` ou dans un fichier dans le `users.d` répertoire (pour plus d’informations, consultez la section “[Fichiers de Configuration](configuration-files.md#configuration_files)”).

La configuration comprend des commentaires expliquant comment ouvrir l’accès de partout.

Pour une utilisation en production, spécifiez uniquement `ip` (adresses IP et leurs masques), depuis l’utilisation `host` et `hoost_regexp` peut causer une latence supplémentaire.

Ensuite, le profil des paramètres utilisateur est spécifié (voir la section “[Les paramètres des profils](settings/settings-profiles.md)”. Vous pouvez spécifier le profil par défaut, `default'`. Le profil peut avoir n’importe quel nom. Vous pouvez spécifier le même profil pour différents utilisateurs. La chose la plus importante que vous pouvez écrire dans les paramètres de profil `readonly=1` qui assure un accès en lecture seule. Spécifiez ensuite le quota à utiliser (voir la section “[Quota](quotas.md#quotas)”). Vous pouvez spécifier le quota par défaut: `default`. It is set in the config by default to only count resource usage, without restricting it. The quota can have any name. You can specify the same quota for different users – in this case, resource usage is calculated for each user individually.

Dans le facultatif `<allow_databases>` section, vous pouvez également spécifier une liste de bases de données que l’utilisateur peut accéder. Par défaut, toutes les bases de données sont disponibles pour l’utilisateur. Vous pouvez spécifier l’ `default` la base de données. Dans ce cas, l’utilisateur recevra l’accès à la base de données par défaut.

Dans le facultatif `<allow_dictionaries>` section, vous pouvez également spécifier une liste de dictionnaires que l’utilisateur peut accéder. Par défaut, tous les dictionnaires sont disponibles pour l’utilisateur.

L’accès à la `system` la base de données est toujours autorisée (puisque cette base de données est utilisée pour traiter les requêtes).

L’utilisateur peut obtenir une liste de toutes les bases de données et tables en utilisant `SHOW` requêtes ou tables système, même si l’accès aux bases de données individuelles n’est pas autorisé.

Accès de base de données n’est pas liée à la [ReadOnly](settings/permissions-for-queries.md#settings_readonly) paramètre. Vous ne pouvez pas accorder un accès complet à une base de données et `readonly` l’accès à un autre.

[Article Original](https://clickhouse.tech/docs/en/operations/access_rights/) <!--hide-->
