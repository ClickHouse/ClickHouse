export const ScalePlanFeatureBadge = ({feature='Cette fonctionnalité', linking_verb_are = false}) => {
    return (
        <div className="scalePlanFeatureContainer">
            <div className="scalePlanFeatureBadge">
                Fonctionnalité de l’offre Scale
            </div>
            <div>
                <p>{feature} {linking_verb_are ? 'sont' : 'est'} disponible{linking_verb_are ? 's' : ''} avec les offres Scale et Enterprise. Pour passer à une offre supérieure, consultez la page des offres dans la console Cloud.</p>
            </div>
        </div>
    )
}