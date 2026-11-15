export const ScalePlanFeatureBadge = ({feature='Esta funcionalidad', linking_verb_are = false}) => {
    return (
        <div className="scalePlanFeatureContainer">
            <div className="scalePlanFeatureBadge">
                Funcionalidad del plan Scale
            </div>
            <div>
                <p>{feature} {linking_verb_are ? 'están disponibles' : 'está disponible'} en los planes Scale y Enterprise. Para cambiar de plan, visita la página de planes en la consola de Cloud.</p>
            </div>
        </div>
    )
}