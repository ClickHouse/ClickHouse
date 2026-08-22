export const EnterprisePlanFeatureBadge = ({feature='Cette fonctionnalité', support=false, linking_verb_are = false}) => {
    return (
        <div className="enterprisePlanFeatureContainer">
            <div className="enterprisePlanFeatureBadge">
                Fonctionnalité du plan Enterprise
            </div>
            <div>
                <p>{feature} {linking_verb_are ? 'sont' : 'est'} disponible{linking_verb_are ? 's' : ''} avec le plan Enterprise. {support ? `Contactez le support pour activer cette fonctionnalité.` : 'Pour passer à une offre supérieure, consultez la page des plans dans la Cloud Console.'}</p>
            </div>
        </div>
    )
}