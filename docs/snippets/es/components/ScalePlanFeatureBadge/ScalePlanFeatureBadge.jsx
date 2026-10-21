export const ScalePlanFeatureBadge = ({feature='Esta característica', linking_verb_are = false}) => {
    return (
        <div className="scalePlanFeatureContainer">
            <div className="scalePlanFeatureBadge">
                Característica del plan Scale
            </div>
            <div>
                <p>{feature} {linking_verb_are ? 'están disponibles' : 'está disponible'} en los planes Scale y Enterprise. Para actualizar el plan, visita la página de planes en Cloud Console.</p>
            </div>
        </div>
    )
}