---
description: 'Documentation de ClickHouse Obfuscator'
slug: /operations/utilities/clickhouse-obfuscator
title: 'clickhouse-obfuscator'
doc_type: 'reference'
---

Un outil simple d&#39;obfuscation des données de table.

Il lit une table d&#39;entrée et produit une table de sortie, qui conserve certaines propriétés de l&#39;entrée, mais contient des données différentes.
Il permet de publier des données de production quasi réelles à utiliser dans des benchmarks.

Il est conçu pour conserver les propriétés suivantes des données :

* les cardinalités des valeurs (nombre de valeurs distinctes) pour chaque colonne et chaque tuple de colonnes ;

* les cardinalités conditionnelles : le nombre de valeurs distinctes d&#39;une colonne sous condition sur la valeur d&#39;une autre colonne ;

* les distributions de probabilité de la valeur absolue des entiers ; du signe des entiers signés ; de l&#39;exposant et du signe des flottants ;

* les distributions de probabilité de la longueur des chaînes ;

* la probabilité que les nombres valent zéro, ainsi que celle des chaînes et des tableaux vides, et des `NULL` ;

* le ratio de compression des données lorsqu&#39;elles sont compressées avec LZ77 et la famille des codecs entropiques ;

* la continuité (ampleur des écarts) des valeurs temporelles dans la table ; la continuité des valeurs à virgule flottante ;

* la composante date des valeurs `DateTime` ;

* la validité UTF-8 des valeurs de chaîne ;

* les valeurs de chaîne paraissent naturelles.

La plupart des propriétés ci-dessus sont utiles pour les tests de performance :

la lecture des données, le filtrage, l&#39;aggregation et le tri fonctionneront à presque la même vitesse
que sur les données d&#39;origine grâce aux cardinalités, amplitudes, ratios de compression, etc. conservés.

Il fonctionne de manière déterministe : vous définissez une valeur de seed, et la transformation est déterminée par les données d&#39;entrée et par cette seed.
Certaines transformations sont bijectives et pourraient être inversées ; vous devez donc utiliser une seed de grande taille et la garder secrète.

Il utilise certaines primitives cryptographiques pour transformer les données, mais d&#39;un point de vue cryptographique, il ne le fait pas correctement. C&#39;est pourquoi vous ne devez pas considérer le résultat comme sûr, sauf si vous avez une autre raison de le faire. Le résultat peut conserver certaines données que vous ne souhaitez pas publier.

Il laisse toujours exactement inchangés les nombres 0, 1 et -1, les dates, les longueurs des tableaux et les indicateurs de NULL des données source.
Par exemple, si vous avez une colonne `IsMobile` dans votre table avec les valeurs 0 et 1, elle aura la même valeur dans les données transformées.

Ainsi, l&#39;utilisateur pourra calculer la proportion exacte du trafic mobile.

Prenons un autre exemple. Si vous avez dans votre table des données privées, comme l&#39;adresse e-mail d&#39;un utilisateur, et que vous ne voulez publier aucune adresse e-mail.
Si votre table est suffisamment grande, contient plusieurs e-mails différents, et qu&#39;aucun e-mail n&#39;a une fréquence bien plus élevée que les autres, l&#39;outil anonymisera toutes les données. Mais si vous avez un petit nombre de valeurs différentes dans une colonne, il peut en reproduire certaines.
Vous devriez examiner le fonctionnement de l&#39;algorithme de cet outil et ajuster finement ses paramètres de ligne de commande.

Cet outil ne fonctionne correctement qu&#39;avec au moins une quantité modérée de données (au moins des milliers de lignes).