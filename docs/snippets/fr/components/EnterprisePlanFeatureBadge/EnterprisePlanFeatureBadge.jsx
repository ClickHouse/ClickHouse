export const EnterprisePlanFeatureBadge = ({feature='Cette fonctionnalité', support=false, linking_verb_are = false}) => {
    return (
        <div className="enterprisePlanFeatureContainer">
            <div className="enterprisePlanFeatureBadge">
                Fonctionnalité du plan Enterprise
            </div>
            <div>
                <p>{feature} {linking_verb_are ? 'sont disponibles' : 'est disponible'} avec le plan Enterprise. {support ? `Contactez l’assistance pour activer cette fonctionnalité.` : 'Pour effectuer la mise à niveau, consultez la page des plans dans la Cloud Console.'}</p>
            </div>
        </div>
    )
}