---
description: 'Règles de style de code pour le développement C++ de ClickHouse'
sidebar_label: 'Guide de style C++'
sidebar_position: 70
slug: /development/style
title: 'Guide de style C++'
doc_type: 'guide'
---

<div id="general-recommendations">
  ## Recommandations générales
</div>

Les points suivants sont des recommandations, et non des exigences.
Si vous modifiez du code, il est logique de respecter la mise en forme du code existant.
Un style de code est nécessaire pour assurer la cohérence. La cohérence facilite la lecture du code et les recherches dans le code.
Bon nombre de règles n&#39;ont pas de justification logique ; elles sont dictées par les pratiques établies.

<div id="formatting">
  ## Formatage
</div>

**1.** La majeure partie de la mise en forme est effectuée automatiquement par `clang-format`.

**2.** L&#39;indentation est de 4 espaces. Configurez votre environnement de développement de sorte qu&#39;une tabulation insère quatre espaces.

**3.** Les accolades ouvrantes et fermantes doivent figurer sur une ligne distincte.

```cpp
inline void readBoolText(bool & x, ReadBuffer & buf)
{
    char tmp = '0';
    readChar(tmp, buf);
    x = tmp != '0';
}
```

**4.** Si le corps entier de la fonction est un seul `statement`, il peut être placé sur une seule ligne. Placez des espaces autour des accolades (en dehors de l&#39;espace en fin de ligne).

```cpp
inline size_t mask() const                { return buf_size() - 1; }
inline size_t place(HashValue x) const    { return x & mask(); }
```

**5.** Pour les fonctions. Ne pas mettre d&#39;espaces autour des parenthèses.

```cpp
void reinsert(const Value & x)
```

```cpp
memcpy(&buf[place_value], &x, sizeof(x));
```

**6.** Dans les expressions `if`, `for`, `while` et autres, une espace est insérée avant la parenthèse ouvrante (contrairement aux appels de fonctions).

```cpp
for (size_t i = 0; i < rows; i += storage.index_granularity)
```

**7.** Ajoutez des espaces autour des opérateurs binaires (`+`, `-`, `*`, `/`, `%`, ...) et de l&#39;opérateur ternaire `?:`.

```cpp
UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');
```

**8.** Si un saut de ligne est saisi, placez l&#39;opérateur sur une nouvelle ligne et augmentez l&#39;indentation qui le précède.

```cpp
if (elapsed_ns)
    message << " ("
        << rows_read_on_server * 1000000000 / elapsed_ns << " rows/s., "
        << bytes_read_on_server * 1000.0 / elapsed_ns << " MB/s.) ";
```

**9.** Vous pouvez utiliser des espaces pour aligner le contenu au sein d&#39;une ligne, si vous le souhaitez.

```cpp
dst.ClickLogID         = click.LogID;
dst.ClickEventID       = click.EventID;
dst.ClickGoodEvent     = click.GoodEvent;
```

**10.** N&#39;utilisez pas d&#39;espaces autour des opérateurs `.`, `->`.

Si nécessaire, l&#39;opérateur peut être renvoyé à la ligne suivante. Dans ce cas, l&#39;indentation qui le précède est augmentée.

**11.** N&#39;utilisez pas d&#39;espace pour séparer les opérateurs unaires (`--`, `++`, `*`, `&`, ...) de l&#39;argument.

**12.** Placez un espace après une virgule, mais pas avant. La même règle s&#39;applique au point-virgule à l&#39;intérieur d&#39;une expression `for`.

**13.** N&#39;utilisez pas d&#39;espaces pour séparer l&#39;opérateur `[]`.

**14.** Dans une expression `template <...>`, insérez un espace entre `template` et `<` ; pas d&#39;espace après `<` ni avant `>`.

```cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
{}
```

**15.** Dans les classes et les structures, écrivez `public`, `private` et `protected` au même niveau que `class/struct`, et indentez le reste du code.

```cpp
template <typename T>
class MultiVersion
{
public:
    /// Version of object for usage. shared_ptr manage lifetime of version.
    using Version = std::shared_ptr<const T>;
    ...
}
```

**16.** Si le même `namespace` est utilisé pour l&#39;ensemble du fichier et qu&#39;il n&#39;y a rien d&#39;autre de significatif, un décalage n&#39;est pas nécessaire à l&#39;intérieur du `namespace`.

**17.** Si le bloc d&#39;un `if`, `for`, `while` ou de toute autre expression ne contient qu&#39;une seule `statement`, les accolades sont facultatives. Placez plutôt la `statement` sur une ligne séparée. Cette règle s&#39;applique également aux `if`, `for`, `while` imbriqués, ...

Mais si l&#39;`instruction` interne contient des accolades ou `else`, le bloc externe doit être entouré d&#39;accolades.

```cpp
/// Finish write.
for (auto & stream : streams)
    stream.second->finalize();
```

**18.** Il ne doit pas y avoir d&#39;espaces en fin de ligne.

**19.** Les fichiers sources sont encodés en UTF-8.

**20.** Les caractères non-ASCII peuvent être utilisés dans les littéraux de chaîne.

```cpp
<< ", " << (timer.elapsed() / chunks_stats.hits) << " μsec/hit.";
```

**21.** N’écrivez pas plusieurs expressions sur une même ligne.

**22.** Regroupez les sections de code à l’intérieur des fonctions et ne les séparez pas par plus d’une ligne vide.

**23.** Séparez les fonctions, les classes, etc. par une ou deux lignes vides.

**24.** `A const` (lorsqu’il est lié à une valeur) doit être écrit avant le nom du type.

```cpp
//correct
const char * pos
const std::string & s
//incorrect
char const * pos
```

**25.** Lors de la déclaration d&#39;un pointeur ou d&#39;une référence, les symboles `*` et `&` doivent être entourés d&#39;espaces de part et d&#39;autre.

```cpp
//correct
const char * pos
//incorrect
const char* pos
const char *pos
```

**26.** Lorsque vous utilisez des types de template, créez-leur un alias avec le mot-clé `using` (sauf dans les cas les plus simples).

Autrement dit, les paramètres du template sont indiqués uniquement dans `using` et ne sont pas répétés dans le code.

`using` peut être déclaré localement, par exemple à l’intérieur d’une fonction.

```cpp
//correct
using FileStreams = std::map<std::string, std::shared_ptr<Stream>>;
FileStreams streams;
//incorrect
std::map<std::string, std::shared_ptr<Stream>> streams;
```

**27.** Ne déclarez pas plusieurs variables de types différents dans une seule instruction.

```cpp
//incorrect
int x, *y;
```

**28.** N&#39;utilisez pas les conversions de type à la C.

```cpp
//incorrect
std::cerr << (int)c <<; std::endl;
//correct
std::cerr << static_cast<int>(c) << std::endl;
```

**29.** Dans les classes et les structs, regroupez séparément les membres et les fonctions au sein de chaque niveau de visibilité.

**30.** Pour les petites classes et les structs, il n’est pas nécessaire de séparer la déclaration de la méthode de son implémentation.

Il en va de même pour les petites méthodes dans toutes les classes ou structs.

Pour les classes et structs Template, ne séparez pas les déclarations de méthodes de leur implémentation (car sinon elles doivent être définies dans la même unité de traduction).

**31.** Vous pouvez couper les lignes à 140 caractères au lieu de 80.

**32.** Utilisez toujours les opérateurs de pré-incrémentation/pré-décrémentation si la post-incrémentation/post-décrémentation n’est pas nécessaire.

```cpp
for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
```

<div id="comments">
  ## Commentaires
</div>

**1.** Veillez à ajouter des commentaires pour toutes les portions de code non triviales.

C’est très important. Rédiger un commentaire peut vous aider à vous rendre compte que le code n&#39;est pas nécessaire ou qu&#39;il est mal conçu.

```cpp
/** Part of piece of memory, that can be used.
  * For example, if internal_buffer is 1MB, and there was only 10 bytes loaded to buffer from file for reading,
  * then working_buffer will have size of only 10 bytes
  * (working_buffer.end() will point to position right after those 10 bytes available for read).
  */
```

**2.** Les commentaires peuvent être aussi détaillés que nécessaire.

**3.** Placez les commentaires avant le code qu’ils décrivent. Dans de rares cas, les commentaires peuvent venir après le code, sur la même ligne.

```cpp
/** Parses and executes the query.
*/
void executeQuery(
    ReadBuffer & istr, /// Where to read the query from (and data for INSERT, if applicable)
    WriteBuffer & ostr, /// Where to write the result
    Context & context, /// DB, tables, data types, engines, functions, aggregate functions...
    BlockInputStreamPtr & query_plan, /// Here could be written the description on how query was executed
    QueryProcessingStage::Enum stage = QueryProcessingStage::Complete /// Up to which stage process the SELECT query
    )
```

**4.** Les commentaires doivent être rédigés exclusivement en anglais.

**5.** Si vous écrivez une bibliothèque, ajoutez dans le fichier d’en-tête principal des commentaires détaillés qui l’expliquent.

**6.** N’ajoutez pas de commentaires qui n’apportent pas d’informations supplémentaires. En particulier, ne laissez pas de commentaire vide comme celui-ci :

```cpp
/*
* Procedure Name:
* Original procedure name:
* Author:
* Date of creation:
* Dates of modification:
* Modification authors:
* Original file name:
* Purpose:
* Intent:
* Designation:
* Classes used:
* Constants:
* Local variables:
* Parameters:
* Date of creation:
* Purpose:
*/
```

L&#39;exemple est tiré de la ressource http://home.tamk.fi/~jaalto/course/coding-style/doc/unmaintainable-code/.

**7.** N&#39;écrivez pas de commentaires inutiles (auteur, date de création, etc.) au début de chaque fichier.

**8.** Les commentaires sur une seule ligne commencent par trois barres obliques : `///`, et les commentaires sur plusieurs lignes commencent par `/**`. Ces commentaires sont considérés comme de la &quot;documentation&quot;.

Remarque : vous pouvez utiliser Doxygen pour générer de la documentation à partir de ces commentaires. Mais Doxygen n&#39;est généralement pas utilisé, car il est plus pratique de parcourir le code dans l&#39;IDE.

**9.** Les commentaires sur plusieurs lignes ne doivent pas comporter de lignes vides au début ni à la fin (à l&#39;exception de la ligne qui ferme un commentaire sur plusieurs lignes).

**10.** Pour commenter du code, utilisez des commentaires simples, pas des commentaires de &quot;documentation&quot;.

**11.** Supprimez les portions de code commentées avant de valider.

**12.** N&#39;utilisez pas de grossièretés dans les commentaires ni dans le code.

**13.** N&#39;utilisez pas de majuscules. N&#39;abusez pas de la ponctuation.

```cpp
/// WHAT THE FAIL???
```

**14.** N’utilisez pas de commentaires pour faire office de délimiteurs.

```cpp
///******************************************************
```

**15.** N&#39;entamez pas de discussions dans les commentaires.

```cpp
/// Why did you do this stuff?
```

**16.** Il n&#39;est pas nécessaire d&#39;ajouter un commentaire à la fin d&#39;un bloc pour expliquer de quoi il s&#39;agit.

```cpp
/// for
```

<div id="names">
  ## Noms
</div>

**1.** Utilisez des lettres minuscules et des underscores dans les noms de variables et de membres de classe.

```cpp
size_t max_block_size;
```

**2.** Pour les noms de fonctions (méthodes), utilisez le camelCase en commençant par une minuscule.

```cpp
std::string getName() const override { return "Memory"; }
```

**3.** Pour les noms de classes (structs), utilisez le CamelCase en commençant par une majuscule. Aucun préfixe autre que I n’est utilisé pour les interfaces.

```cpp
class StorageMemory : public IStorage
```

**4.** Les `using` sont nommés selon les mêmes règles que les classes.

**5.** Noms des arguments de type de Template : dans les cas simples, utilisez `T` ; `T`, `U` ; `T1`, `T2`.

Pour les cas plus complexes, suivez soit les règles de nommage des classes, soit ajoutez le préfixe `T`.

```cpp
template <typename TKey, typename TValue>
struct AggregatedStatElement
```

**6.** Noms des arguments constants de template : ils doivent soit suivre les règles de nommage des variables, soit utiliser `N` dans les cas simples.

```cpp
template <bool without_www>
struct ExtractDomain
```

**7.** Pour les classes abstraites (interfaces), vous pouvez ajouter le préfixe `I`.

```cpp
class IProcessor
```

**8.** Si vous utilisez une variable localement, vous pouvez utiliser un nom court.

Dans tous les autres cas, utilisez un nom qui décrit son sens.

```cpp
bool info_successfully_loaded = false;
```

**9.** Les noms des `define` et des constantes globales s’écrivent en MAJUSCULES avec des traits de soulignement.

```cpp
#define MAX_SRC_TABLE_NAMES_TO_STORE 1000
```

**10.** Les noms de fichiers doivent suivre le même style que leur contenu.

Si un fichier contient une seule classe, nommez-le comme la classe (CamelCase).

Si le fichier contient une seule fonction, nommez-le comme la fonction (camelCase).

**11.** Si le nom contient une abréviation :

* Pour les noms de variables, l’abréviation doit être en minuscules : `mysql_connection` (et non `mySQL_connection`).
* Pour les noms de classes et de fonctions, conservez les majuscules dans l’abréviation : `MySQLConnection` (et non `MySqlConnection`).

**12.** Les arguments du constructeur servant uniquement à initialiser les membres de la classe doivent porter le même nom que ces membres, mais avec un underscore à la fin.

```cpp
FileQueueProcessor(
    const std::string & path_,
    const std::string & prefix_,
    std::shared_ptr<FileHandler> handler_)
    : path(path_),
    prefix(prefix_),
    handler(handler_),
    log(&Logger::get("FileQueueProcessor"))
{
}
```

Le suffixe underscore peut être omis si l’argument n’est pas utilisé dans le corps du constructeur.

**13.** Il n’y a pas de différence entre les noms des variables locales et ceux des membres de classe (aucun préfixe n’est nécessaire).

```cpp
timer (not m_timer)
```

**14.** Pour les constantes d’un `enum`, utilisez la notation CamelCase commençant par une majuscule. ALL&#95;CAPS est également accepté. Si l’`enum` n’est pas local, utilisez un `enum class`.

```cpp
enum class CompressionMethod
{
    QuickLZ = 0,
    LZ4     = 1,
};
```

**15.** Tous les noms doivent être en anglais. La translittération des mots hébreux n&#39;est pas autorisée.

pas T&#95;PAAMAYIM&#95;NEKUDOTAYIM

**16.** Les abréviations sont acceptables si elles sont bien connues (c&#39;est-à-dire lorsque l&#39;on peut facilement en trouver la signification dans Wikipedia ou via un moteur de recherche).

`AST`, `SQL`.

Pas `NVDH` (une suite de lettres aléatoires)

Les mots tronqués sont acceptables si leur forme abrégée est d&#39;usage courant.

Vous pouvez également utiliser une abréviation si le nom complet figure à côté dans les commentaires.

**17.** Les noms de fichiers contenant du code source C++ doivent avoir l&#39;extension `.cpp`. Les fichiers d’en-tête doivent avoir l&#39;extension `.h`.

<div id="how-to-write-code">
  ## Comment écrire du code
</div>

**1.** Gestion de la mémoire.

La libération manuelle de la mémoire (`delete`) ne peut être utilisée que dans le code de bibliothèque.

Dans le code de bibliothèque, l’opérateur `delete` ne peut être utilisé que dans les destructeurs.

Dans le code applicatif, la mémoire doit être libérée par l’objet qui en a la propriété.

Exemples :

* Le plus simple est de placer un objet sur la pile, ou d’en faire un membre d’une autre classe.
* Pour un grand nombre de petits objets, utilisez des conteneurs.
* Pour la libération automatique d’un petit nombre d’objets alloués sur le tas, utilisez `shared_ptr/unique_ptr`.

**2.** Gestion des ressources.

Utilisez `RAII` et reportez-vous à ce qui précède.

**3.** Gestion des erreurs.

Utilisez des exceptions. Dans la plupart des cas, il suffit de lever une exception, sans avoir besoin de l’intercepter (grâce à `RAII`).

Dans les applications de traitement de données hors ligne, il est souvent acceptable de ne pas intercepter les exceptions.

Dans les serveurs qui traitent les requêtes des utilisateurs, il suffit généralement d’intercepter les exceptions au niveau supérieur du gestionnaire de connexion.

Dans les fonctions exécutées dans des threads, vous devez intercepter et conserver toutes les exceptions afin de les relancer dans le thread principal après `join`.

```cpp
/// If there weren't any calculations yet, calculate the first block synchronously
if (!started)
{
    calculate();
    started = true;
}
else /// If calculations are already in progress, wait for the result
    pool.wait();

if (exception)
    exception->rethrow();
```

Ne masquez jamais les exceptions sans les traiter. Ne consignez jamais aveuglément toutes les exceptions dans les logs.

```cpp
//Not correct
catch (...) {}
```

Si vous devez ignorer certaines exceptions, faites-le uniquement pour des exceptions spécifiques et relancez toutes les autres.

```cpp
catch (const DB::Exception & e)
{
    if (e.code() == ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION)
        return nullptr;
    else
        throw;
}
```

Lorsque vous utilisez des fonctions avec des codes de réponse ou `errno`, vérifiez toujours le résultat et levez une exception en cas d&#39;erreur.

```cpp
if (0 != close(fd))
    throw ErrnoException(ErrorCodes::CANNOT_CLOSE_FILE, "Cannot close file {}", file_name);
```

Vous pouvez utiliser assert pour vérifier un invariant dans le code.

**4.** Types d’exception.

Il n’est pas nécessaire d’utiliser une hiérarchie d’exceptions complexe dans le code applicatif. Le texte de l’exception doit être compréhensible pour un administrateur système.

**5.** Lever des exceptions depuis les destructeurs.

Ce n’est pas recommandé, mais c’est autorisé.

Utilisez les options suivantes :

* Créez une fonction (`done()` ou `finalize()`) qui effectuera à l’avance tout le travail susceptible d’entraîner une exception. Si cette fonction a été appelée, il ne devrait plus y avoir d’exception dans le destructeur.
* Les tâches trop complexes (comme l’envoi de messages sur le réseau) peuvent être confiées à une méthode distincte que l’utilisateur de la classe devra appeler avant la destruction.
* S’il y a une exception dans le destructeur, il vaut mieux la consigner dans les logs que la masquer (si le logger est disponible).
* Dans les applications simples, il est acceptable de s’appuyer sur `std::terminate` (pour les cas de `noexcept` par défaut en C++11) pour gérer les exceptions.

**6.** Blocs de code anonymes.

Vous pouvez créer un bloc de code distinct au sein d’une même fonction afin de rendre certaines variables locales, de sorte que les destructeurs soient appelés à la sortie du bloc.

```cpp
Block block = data.in->read();

{
    std::lock_guard<std::mutex> lock(mutex);
    data.ready = true;
    data.block = block;
}

ready_any.set();
```

**7.** Multithreading.

Dans les programmes de traitement de données hors ligne :

* Essayez d’obtenir les meilleures performances possibles sur un seul cœur de CPU. Vous pourrez ensuite paralléliser votre code si nécessaire.

Dans les applications serveur :

* Utilisez le pool de threads pour traiter les requêtes. À ce stade, nous n’avons eu aucune tâche nécessitant un changement de contexte en espace utilisateur.

Fork n’est pas utilisé pour la parallélisation.

**8.** Synchronisation des threads.

Il est souvent possible de faire en sorte que différents threads utilisent différentes cellules mémoire (ou, mieux encore, différentes lignes de cache) et de ne recourir à aucune synchronisation entre threads (sauf `joinAll`).

Si une synchronisation est nécessaire, dans la plupart des cas, il suffit d’utiliser un mutex avec `lock_guard`.

Dans les autres cas, utilisez les primitives de synchronisation du système. N’utilisez pas d’attente active.

Les opérations atomiques ne doivent être utilisées que dans les cas les plus simples.

N’essayez pas d’implémenter des structures de données sans verrou, sauf si c’est votre principal domaine d’expertise.

**9.** Pointeurs vs références.

Dans la plupart des cas, privilégiez les références.

**10.** `const`.

Utilisez des références constantes, des pointeurs vers des constantes, `const_iterator` et des méthodes `const`.

Considérez `const` comme le choix par défaut et n’utilisez le non-`const` qu’en cas de nécessité.

Lors du passage de variables par valeur, utiliser `const` n’a généralement pas de sens.

**11.** unsigned.

Utilisez `unsigned` si nécessaire.

**12.** Types numériques.

Utilisez les types `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Int8`, `Int16`, `Int32` et `Int64`, ainsi que `size_t`, `ssize_t` et `ptrdiff_t`.

N&#39;utilisez pas ces types pour les nombres : `signed/unsigned long`, `long long`, `short`, `signed/unsigned char`, `char`.

**13.** Passage des arguments.

Passez les valeurs complexes par valeur si elles doivent être déplacées, et utilisez `std::move` ; passez-les par référence si vous souhaitez mettre à jour une valeur dans une boucle.

Si une fonction prend possession d’un objet créé sur le tas, donnez à l’argument le type `shared_ptr` ou `unique_ptr`.

**14.** Valeurs de retour.

Dans la plupart des cas, utilisez simplement `return`. N’écrivez pas `return std::move(res)`.

Si la fonction alloue un objet sur le tas et le renvoie, utilisez `shared_ptr` ou `unique_ptr`.

Dans de rares cas (mise à jour d’une valeur dans une boucle), vous devrez peut-être renvoyer la valeur via un argument. Dans ce cas, l’argument doit être une référence.

```cpp
using AggregateFunctionPtr = std::shared_ptr<IAggregateFunction>;

/** Allows creating an aggregate function by its name.
  */
class AggregateFunctionFactory
{
public:
    AggregateFunctionFactory();
    AggregateFunctionPtr get(const String & name, const DataTypes & argument_types) const;
```

**15.** `namespace`.

Il n’est pas nécessaire d’utiliser un `namespace` distinct pour le code applicatif.

Les petites bibliothèques n’en ont pas non plus besoin.

Pour les bibliothèques de taille moyenne ou grande, placez tout dans un `namespace`.

Dans le fichier `.h` de la bibliothèque, vous pouvez utiliser `namespace detail` pour masquer les détails d’implémentation dont le code applicatif n’a pas besoin.

Dans un fichier `.cpp`, vous pouvez utiliser le mot-clé `static` ou un `namespace` anonyme pour masquer des symboles.

De plus, un `namespace` peut être utilisé avec un `enum` pour éviter que les noms correspondants ne se retrouvent dans un `namespace` externe (mais il est préférable d’utiliser un `enum class`).

**16.** Initialisation différée.

Si des arguments sont requis pour l’initialisation, vous ne devriez normalement pas écrire de constructeur par défaut.

Si, plus tard, vous devez différer l’initialisation, vous pouvez ajouter un constructeur par défaut qui créera un objet invalide. Ou, pour un petit nombre d’objets, vous pouvez utiliser `shared_ptr/unique_ptr`.

```cpp
Loader(DB::Connection * connection_, const std::string & query, size_t max_block_size_);

/// For deferred initialization
Loader() {}
```

**17.** Fonctions virtuelles.

Si la classe n’est pas destinée à un usage polymorphe, il n’est pas nécessaire de déclarer les fonctions comme virtuelles. Cela vaut aussi pour le destructeur.

**18.** Encodages.

Utilisez UTF-8 partout. Utilisez `std::string` et `char *`. N’utilisez pas `std::wstring` ni `wchar_t`.

**19.** Journalisation.

Voir les exemples dans l’ensemble du code.

Avant de valider, supprimez toute journalisation inutile ou de débogage, ainsi que tout autre type de sortie de débogage.

La journalisation dans les boucles est à éviter, même au niveau Trace.

Les journaux doivent rester lisibles à n’importe quel niveau de journalisation.

Dans la plupart des cas, la journalisation ne doit être utilisée que dans le code applicatif.

Les messages de journal doivent être rédigés en anglais.

Le journal devrait de préférence être compréhensible pour l’administrateur système.

N’utilisez pas de grossièretés dans le journal.

Utilisez l’encodage UTF-8 dans le journal. Dans de rares cas, vous pouvez utiliser des caractères non ASCII dans le journal.

**20.** Entrées-sorties.

N’utilisez pas `iostreams` dans les boucles internes critiques pour les performances de l’application (et n’utilisez jamais `stringstream`).

Utilisez plutôt la bibliothèque `DB/IO`.

**21.** Date et heure.

Voir la bibliothèque `DateLUT`.

**22.** include.

Utilisez toujours `#pragma once` plutôt que des gardes d’inclusion.

**23.** using.

`using namespace` ne doit pas être utilisé. Vous pouvez utiliser `using` pour quelque chose de spécifique. Mais limitez-le à une portée locale, à l’intérieur d’une classe ou d’une fonction.

**24.** N’utilisez pas `trailing return type` pour les fonctions, sauf si nécessaire.

```cpp
auto f() -> void
```

**25.** Déclaration et initialisation de variables.

```cpp
//right way
std::string s = "Hello";
std::string s{"Hello"};

//wrong way
auto s = std::string{"Hello"};
```

**26.** Pour les fonctions virtuelles, utilisez `virtual` dans la classe de base, mais `override` plutôt que `virtual` dans les classes dérivées.

<div id="unused-features-of-c">
  ## Fonctionnalités inutilisées de C++
</div>

**1.** L’héritage virtuel n’est pas utilisé.

**2.** Les constructions qui bénéficient d’une syntaxe simplifiée en C++ moderne, par ex.

```cpp
// Traditional way without syntactic sugar
template <typename G, typename = std::enable_if_t<std::is_same<G, F>::value, void>> // SFINAE via std::enable_if, usage of ::value
std::pair<int, int> func(const E<G> & e) // explicitly specified return type
{
    if (elements.count(e)) // .count() membership test
    {
        // ...
    }

    elements.erase(
        std::remove_if(
            elements.begin(), elements.end(),
            [&](const auto x){
                return x == 1;
            }),
        elements.end()); // remove-erase idiom

    return std::make_pair(1, 2); // create pair via make_pair()
}

// With syntactic sugar (C++14/17/20)
template <typename G>
requires std::same_v<G, F> // SFINAE via C++20 concept, usage of C++14 template alias
auto func(const E<G> & e) // auto return type (C++14)
{
    if (elements.contains(e)) // C++20 .contains membership test
    {
        // ...
    }

    elements.erase_if(
        elements,
        [&](const auto x){
            return x == 1;
        }); // C++20 std::erase_if

    return {1, 2}; // or: return std::pair(1, 2); // create pair via initialization list or value initialization (C++17)
}
```

<div id="platform">
  ## Plateforme
</div>

**1.** Nous écrivons du code pour une plateforme spécifique.

Mais, toutes choses égales par ailleurs, le code multiplateforme ou portable est à privilégier.

**2.** Langage : C++20 (voir la liste des [fonctionnalités C++20 disponibles](https://en.cppreference.com/w/cpp/compiler_support#C.2B.2B20_features)).

**3.** Compilateur : `clang`. Au moment de la rédaction (mars 2025), le code est compilé avec une version de clang &gt;= 19.

La bibliothèque standard est utilisée (`libc++`).

**4.** OS : Linux Ubuntu, pas antérieur à Precise.

**5.** Le code est écrit pour l&#39;architecture CPU x86&#95;64.

Le jeu d&#39;instructions du CPU correspond au minimum pris en charge sur nos serveurs. Actuellement, il s&#39;agit de SSE 4.2.

**6.** Utilisez les flags de compilation `-Wall -Wextra -Werror -Weverything`, à quelques exceptions près.

**7.** Utilisez la liaison statique avec toutes les bibliothèques, sauf celles qu&#39;il est difficile de lier statiquement (voir la sortie de la commande `ldd`).

**8.** Le code est développé et débogué avec les paramètres de compilation release.

<div id="tools">
  ## Outils
</div>

**1.** KDevelop est un bon IDE.

**2.** Pour le débogage, utilisez `gdb`, `valgrind` (`memcheck`), `strace`, `-fsanitize=...` ou `tcmalloc_minimal_debug`.

**3.** Pour le profilage, utilisez `Linux Perf`, `valgrind` (`callgrind`) ou `strace -cf`.

**4.** Le code source est dans Git.

**5.** La compilation utilise `CMake`.

**6.** Les programmes sont distribués sous forme de paquets `deb`.

**7.** Les commits sur master ne doivent pas casser le build.

Seules certaines révisions sont toutefois considérées comme viables.

**8.** Faites des commits aussi souvent que possible, même si le code n&#39;est que partiellement prêt.

Utilisez des branches à cette fin.

Si votre code dans la branche `master` n&#39;est pas encore compilable, excluez-le du build avant le `push`. Vous devrez le terminer ou le supprimer dans les jours qui suivent.

**9.** Pour les modifications non triviales, utilisez des branches et publiez-les sur le serveur.

**10.** Le code inutilisé est supprimé du dépôt.

<div id="libraries">
  ## Bibliothèques
</div>

**1.** La bibliothèque standard C++20 est utilisée (les extensions expérimentales sont autorisées), ainsi que les bibliothèques `boost` et `Poco`.

**2.** L&#39;utilisation de bibliothèques issues de paquets du système d&#39;exploitation n&#39;est pas autorisée. L&#39;utilisation de bibliothèques préinstallées n&#39;est pas non plus autorisée. Toutes les bibliothèques doivent être placées sous forme de code source dans le répertoire `contrib` et compilées avec ClickHouse. Voir [Recommandations pour ajouter de nouvelles bibliothèques tierces](/fr/development/contrib#adding-and-maintaining-third-party-libraries) pour plus de détails.

**3.** La préférence va toujours aux bibliothèques déjà utilisées.

<div id="general-recommendations">
  ## Recommandations générales
</div>

**1.** Écrivez le moins de code possible.

**2.** Privilégiez la solution la plus simple.

**3.** N’écrivez pas de code tant que vous ne savez pas comment il va fonctionner ni comment la boucle interne se comportera.

**4.** Dans les cas les plus simples, utilisez `using` plutôt que des classes ou des structs.

**5.** Si possible, n’écrivez pas de constructeurs de copie, d’opérateurs d’affectation, de destructeurs (autres qu’un destructeur virtuel, si la classe contient au moins une fonction virtuelle), de constructeurs de déplacement ni d’opérateurs d’affectation par déplacement. Autrement dit, les fonctions générées par le compilateur doivent fonctionner correctement. Vous pouvez utiliser `default`.

**6.** Simplifier le code est encouragé. Réduisez-en la taille lorsque c’est possible.

<div id="additional-recommendations">
  ## Recommandations supplémentaires
</div>

**1.** Il n’est pas recommandé d’indiquer explicitement `std::` pour les types de `stddef.h`

Autrement dit, nous recommandons d’écrire `size_t` plutôt que `std::size_t`, car c’est plus court.

Il est acceptable d’ajouter `std::`.

**2.** Il n’est pas recommandé d’indiquer explicitement `std::` pour les fonctions de la bibliothèque C standard

Autrement dit, écrivez `memcpy` plutôt que `std::memcpy`.

La raison est qu’il existe des fonctions non standard similaires, comme `memmem`. Il nous arrive d’utiliser ces fonctions. Elles n’existent pas dans `namespace std`.

Si vous écrivez systématiquement `std::memcpy` au lieu de `memcpy`, alors `memmem` sans `std::` paraîtra étrange.

Néanmoins, vous pouvez utiliser `std::` si vous le préférez.

**3.** Utiliser des fonctions C lorsque les mêmes existent dans la bibliothèque C++ standard.

C’est acceptable si c’est plus efficace.

Par exemple, utilisez `memcpy` plutôt que `std::copy` pour copier de gros blocs de mémoire.

**4.** Arguments de fonction sur plusieurs lignes.

N’importe lequel des styles de retour à la ligne suivants est autorisé :

```cpp
function(
  T1 x1,
  T2 x2)
```

```cpp
function(
  size_t left, size_t right,
  const & RangesInDataParts ranges,
  size_t limit)
```

```cpp
function(size_t left, size_t right,
  const & RangesInDataParts ranges,
  size_t limit)
```

```cpp
function(size_t left, size_t right,
      const & RangesInDataParts ranges,
      size_t limit)
```

```cpp
function(
      size_t left,
      size_t right,
      const & RangesInDataParts ranges,
      size_t limit)
```