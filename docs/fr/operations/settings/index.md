---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: "Param\xE8tre"
toc_priority: 55
toc_title: Introduction
---

# Paramètre {#session-settings-intro}

Il existe plusieurs façons d'effectuer tous les paramètres décrits dans cette section de la documentation.

Les paramètres sont configurés en couches, de sorte que chaque couche suivante redéfinit les paramètres précédents.

Façons de configurer les paramètres, par ordre de priorité:

-   Paramètres dans l' `users.xml` fichier de configuration du serveur.

    Situé dans l'élément `<profiles>`.

-   Les paramètres de la Session.

    Envoyer `SET setting=value` depuis le client de la console ClickHouse en mode interactif.
    De même, vous pouvez utiliser des sessions ClickHouse dans le protocole HTTP. Pour ce faire, vous devez spécifier le `session_id` Paramètre HTTP.

-   Les paramètres de requête.

    -   Lorsque vous démarrez le client clickhouse console en mode non interactif, définissez le paramètre startup `--setting=value`.
    -   Lors de l'utilisation de L'API HTTP, passez les paramètres CGI (`URL?setting_1=value&setting_2=value...`).

Les paramètres qui ne peuvent être effectués que dans le fichier de configuration du serveur ne sont pas couverts dans cette section.

[Article Original](https://clickhouse.tech/docs/en/operations/settings/) <!--hide-->
