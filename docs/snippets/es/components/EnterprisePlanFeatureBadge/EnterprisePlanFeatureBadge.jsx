export const EnterprisePlanFeatureBadge = ({feature='Esta funcionalidad', support=false, linking_verb_are = false}) => {
    return (
        <div className="enterprisePlanFeatureContainer">
            <div className="enterprisePlanFeatureBadge">
                Funcionalidad del plan Enterprise
            </div>
            <div>
                <p>{feature} {linking_verb_are ? 'están' : 'está'} disponible{linking_verb_are ? 's' : ''} en el plan Enterprise. {support ? `Contacta con soporte para habilitar esta funcionalidad.` : 'Para cambiar de plan, visita la página de planes en la consola de Cloud.'}</p>
            </div>
        </div>
    )
}